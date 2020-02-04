package main

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	// How long to try to publish a message before giving up.
	publishTimeout = time.Second * 10

	// Default value for Broker.sendTimeout. This essentially sets a ceiling on time to deliver a message,
	defaultSendTimeout = time.Millisecond * 500
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func main() {
	// Graceful shutdown on interrupt.
	shutdownCtx, shutdown := context.WithCancel(context.Background())
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		log.Println("Gracefully shutting down...")
		shutdown()
		for range interrupt {
		}
	}()

	b := NewBroker(shutdownCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("/pub", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeHttpErr(w, http.StatusMethodNotAllowed)
			return
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			writeHttpErr(w, http.StatusBadRequest)
			return
		}
		// TODO: Request body size limits.
		if len(body) == 0 || !utf8.Valid(body) {
			writeHttpErr(w, http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
		defer cancel()
		if sent := b.Send(ctx, string(body)); !sent {
			writeHttpErr(w, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/sub", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			// TODO: Better error handling??
			return
		}

		ctx, cancel := context.WithCancel(shutdownCtx)
		defer cancel()

		// Start a reader goroutine that just discards messages. This is
		// necessary for automatic handling of ping/pong/close messages
		// according to the gorilla docs.
		go func() {
			for {
				if _, _, err := c.NextReader(); err != nil {
					c.Close()
					cancel()
					break
				}
			}
		}()

		err = b.Subscribe(ctx, func(msg string) {
			if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				log.Println(err)
				cancel()
			}
		})

		// Subscription has been canceled, but it could have been a client
		// close, a context close, or a send timeout, so try and write the
		// close message in any case.
		if err == ErrSubTimeout {
			c.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "consuming too slow"),
				time.Now().Add(time.Second*5),
			)
		} else {
			c.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseServiceRestart, "shutting down"),
				time.Now().Add(time.Second*5),
			)
		}
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Println("Starting...")
		srv.ListenAndServe()
	}()
	<-shutdownCtx.Done()
	if err := gracefulShutdownServer(srv, time.Second*30); err != nil {
		log.Println(err)
	}

}

func gracefulShutdownServer(srv *http.Server, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return srv.Shutdown(ctx)
}

func writeHttpErr(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

type Broker struct {
	// Controlling context for the Broker's main goroutine.
	ctx context.Context

	// Input channel for messages.
	messages chan string

	// Set of active subscriptions.
	subscribers map[*subscription]struct{}

	// Input channel for new subscriptions.
	subscribeChan chan *subscription

	// Input channel for subscriptions to drop.
	unsubscribeChan chan *subscription

	// Timeout for how long to attempt sending to a subscription before giving
	// up and dropping it. Configurable for testing purposes.
	sendTimeout time.Duration
}

// Creates and starts a new Broker. The passed context controls the lifetime
// of the Broker's main goroutine.
func NewBroker(ctx context.Context) *Broker {
	b := &Broker{
		ctx:             ctx,
		messages:        make(chan string, 1000),
		subscribers:     make(map[*subscription]struct{}),
		subscribeChan:   make(chan *subscription),
		unsubscribeChan: make(chan *subscription),
		sendTimeout:     defaultSendTimeout,
	}
	go b.loop()
	return b
}

// Send the passed message out to all subscribers. Returns whether the message
// was accepted or dropped. Messages may be dropped if the Broker is in the
// process of shutting down, or if the passed context is canceled before the
// message can be buffered.
func (b *Broker) Send(ctx context.Context, msg string) bool {
	select {
	case b.messages <- msg:
		return true
	case <-b.ctx.Done():
		return false
	case <-ctx.Done():
		return false
	}
}

type subscription struct {
	messages chan string
	canceled chan struct{}
	err      error
}

var ErrSubTimeout = errors.New("subscription was canceled for blocking for too long")

// Registers a callback to be called when new messages come in. Blocks until
// the subscription is released using the passed context.
func (b *Broker) Subscribe(
	ctx context.Context,
	callback func(message string),
) error {

	// Create buffered channel for the Broker to send messages back on.
	messages := make(chan string, 20)
	defer close(messages)

	// Create done channel for the Broker to signal that it's decided to
	// cancel this subscription.
	canceled := make(chan struct{})

	s := &subscription{
		messages,
		canceled,
		nil,
	}

	// Subscribe, making sure to break when the controlling or subscription
	// contexts are canceled.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.subscribeChan <- s:
	}

	for {
		select {
		case msg := <-messages:
			callback(msg)
		case <-canceled:
			// Broker canceled this subscription and should have handled
			// unsubscribing, so we can just return the associated error.
			return s.err
		case <-ctx.Done():
			// We want to unsubscribe at this point, but need to continue
			// receiving from messages since the main Broker loop will block
			// if it's unable to send, and that could happen before the
			// unsubscribe message is handled. Because unsubscribeChan is
			// unbuffered, we know that once the value is received nothing
			// else will be sent on messages and it's safe to close.
			for {
				select {
				case <-messages:
					// drop
				case <-canceled:
					return ctx.Err()
				case b.unsubscribeChan <- s:
					return ctx.Err()
				}
			}
		}
	}
}

func (b *Broker) loop() {
	for {
		select {
		case s := <-b.subscribeChan:
			b.subscribers[s] = struct{}{}
		case s := <-b.unsubscribeChan:
			b.unsubscribe(s, nil)
		case msg := <-b.messages:
			// Though subscribers are all buffered, slow subscribers could
			// still block. Dropping messages would be easiest, but we're
			// going to do the arguably more correct thing and just cancel any
			// especially slow subscriptions. Because all of the receivers
			// should be making progress concurrently, we can use a shared
			// timeout and cancel all subscriptions that don't successfully
			// send before it elapses.
			//
			// Note that this will only happen if there are a lot of
			// concurrent messages being sent, since the subscription's
			// message channel needs to be overflowing in the first place.
			var timedout bool
			timeout := time.NewTimer(b.sendTimeout)
			for s, _ := range b.subscribers {
				if !timedout {
					select {
					case s.messages <- msg:
					case <-timeout.C:
						timedout = true
						b.unsubscribe(s, ErrSubTimeout)
					}
				} else {
					// Timeout already elapsed, just unsubscribe if the send
					// doesn't complete synchronously.
					select {
					case s.messages <- msg:
					default:
						b.unsubscribe(s, ErrSubTimeout)
					}
				}
			}
			if !timedout && !timeout.Stop() {
				<-timeout.C
			}
		case <-b.ctx.Done():
			// Cancel all subscriptions.
			err := b.ctx.Err()
			for s, _ := range b.subscribers {
				b.unsubscribe(s, err)
			}
			return
		}
	}
}

// Removes the passed subscription from the set of subscribers, and closes its
// canceled chan if it hasn't already been closed.
func (b *Broker) unsubscribe(s *subscription, err error) {
	if _, ok := b.subscribers[s]; ok {
		delete(b.subscribers, s)
		s.err = err
		close(s.canceled)
	}
}
