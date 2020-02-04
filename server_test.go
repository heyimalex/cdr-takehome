package main

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func send(b *Broker, msg string) bool {
	return b.Send(context.TODO(), msg)
}

func TestBrokerBasic(t *testing.T) {
	require := require.New(t)

	brokerCtx, brokerCancel := context.WithCancel(context.Background())
	defer brokerCancel()

	b := NewBroker(brokerCtx)
	var wg sync.WaitGroup
	wg.Add(1)

	send(b, "no-subs")
	time.Sleep(1)

	ctx, cancel := context.WithCancel(context.Background())
	var values []string
	var err error
	go func() {
		defer wg.Done()
		err = b.Subscribe(ctx, func(msg string) {
			values = append(values, msg)
		})
	}()
	time.Sleep(1)
	send(b, "foo")
	send(b, "bar")
	send(b, "baz")
	time.Sleep(1)

	cancel()
	wg.Wait()
	send(b, "should-be-unsubed")

	require.Equal(context.Canceled, err)
	require.Equal([]string{"foo", "bar", "baz"}, values)
}

func TestBrokerSlowSubscriberDropped(t *testing.T) {
	require := require.New(t)

	brokerCtx, brokerCancel := context.WithCancel(context.Background())
	defer brokerCancel()

	b := NewBroker(brokerCtx)
	b.sendTimeout = time.Millisecond

	var wg sync.WaitGroup
	wg.Add(2)

	ctx, cancel := context.WithCancel(context.Background())
	var values []string
	var err error
	go func() {
		defer wg.Done()
		err = b.Subscribe(ctx, func(msg string) {
			values = append(values, msg)
		})
	}()
	var slowValues []string
	var slowErr error
	go func() {
		defer wg.Done()
		slowErr = b.Subscribe(ctx, func(msg string) {
			time.Sleep(time.Millisecond * 2)
			slowValues = append(slowValues, msg)
		})
	}()
	time.Sleep(1)
	var expected []string
	iterations := 100
	for i := 0; i < iterations; i++ {
		msg := strconv.Itoa(i)
		send(b, msg)
		expected = append(expected, msg)
	}
	time.Sleep(time.Millisecond * time.Duration(iterations+1))

	cancel()
	wg.Wait()

	require.Equal(context.Canceled, err)
	require.Equal(expected, values)

	require.Equal(ErrSubTimeout, slowErr)
	require.Less(len(slowValues), len(values), "should be less values in the slow subscriber")
	require.Greater(len(slowValues), 0, "should be _some_ values in the slow subscriber")
}

// TODO: Racy integration test...
