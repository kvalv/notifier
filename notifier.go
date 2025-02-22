package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
)

type Topic string

type Notifier struct {
	ctx  context.Context
	conn *pgx.Conn
	log  *slog.Logger

	// The list of subscribers, grouped by topic
	subscribers map[Topic][]*Subscription

	// a monotonically increasing counter; each distinct Subscription is assigned
	// an id (which we use when unsubscribing).
	seq int

	// Lock when updating subscribers
	mu sync.Mutex

	doneListen   chan struct{}
	cancelListen context.CancelFunc
	// do not wait for any notifications until we have at least one subscriber
	listening bool
}

func NewNotifier(ctx context.Context, conn *pgx.Conn, log *slog.Logger) *Notifier {
	// ctxListen, cancel := context.WithCancel(ctx)
	return &Notifier{
		ctx:         ctx,
		conn:        conn,
		log:         log,
		subscribers: make(map[Topic][]*Subscription),
		doneListen:  make(chan struct{}, 1),
	}
}

// Creates a new Subscription to topic. Once a notification on the topic is received,
// callback is invoked. This function will set up the topic subscription before
// returning. If the Notifier is already listening to the topic, it will not re-issue
// any LISTEN command.
func (n *Notifier) Subscribe(topic Topic) (*Subscription, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// ... TODO: no need to cancel if we're already listening to the topic
	if n.listening {
		n.cancelListen()
		<-n.doneListen
	}

	if err := n.cmdf("LISTEN %s", topic); err != nil {
		return nil, err
	}

	if !n.listening {
		go n.run()
	}

	id := n.seq
	n.seq++
	s := &Subscription{
		ch:    make(chan string, 100),
		id:    id,
		unsub: func() error { return n.unsubscribe(topic, id) },
	}
	n.addSubscriber(topic, s)

	return s, nil
}

func (n *Notifier) unsubscribe(topic Topic, id int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if subs, ok := n.subscribers[topic]; ok {
		del := func(s *Subscription) bool { return s.id == id }
		n.subscribers[topic] = slices.DeleteFunc(subs, del)
		if len(n.subscribers[topic]) == 0 {
			return n.cmdf("UNLISTEN %s", topic)
		}
	}
	return nil
}

func (n *Notifier) addSubscriber(topic Topic, s *Subscription) {
	if _, ok := n.subscribers[topic]; !ok {
		n.subscribers[topic] = []*Subscription{}
	}
	n.subscribers[topic] = append(n.subscribers[topic], s)
}

// Executes a command on the connection
func (n *Notifier) cmdf(format string, a ...any) error {
	sql := fmt.Sprintf(format, a...)
	_, err := n.conn.Exec(n.ctx, sql)
	return err
}

// run listens for notifications and relays them to subscribers
func (n *Notifier) run() {
	if n.listening {
		panic("already listening")
	}
	n.listening = true
	ctx, cancel := context.WithCancel(n.ctx)
	n.cancelListen = cancel
	defer func() {
		n.log.Info("listening done")
		n.listening = false
		n.doneListen <- struct{}{}
	}()
	for {
		n.log.Info("waiting for notification")
		notif, err := n.conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil { // do not log if context is cancelled
				return
			}
			// check if conn is closed
			if n.conn.IsClosed() {
				return
			}

			n.log.Error("failed to get conn", "err", err)
			return
		}
		n.log.Info("received notification", "channel", notif.Channel, "payload", notif.Payload)

		subs, _ := n.subscribers[Topic(notif.Channel)]
		for _, s := range subs {
			n.log.Info("sending notification", "count", len(subs), "payload", notif.Payload)
			s.ch <- notif.Payload
		}
	}
}

func (n *Notifier) Close() error {
	n.conn.Close(n.ctx)
	n.cancelListen()
	if n.listening {
		<-n.doneListen
	}
	close(n.doneListen)
	return nil
}
