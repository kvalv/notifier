package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
)

type Topic = string

type Notifier struct {
	ctx  context.Context
	conn *pgx.Conn
	log  *slog.Logger

	// The list of subscribers, grouped by topic
	subscribers map[Topic][]*Subscription

	// a monotonically increasing counter; each distinct Subscription is assigned
	// an id (which we use when unsubscribing).
	seq int

	// Lock when updating subscribers or otherwise update the state
	mu sync.Mutex
	wg sync.WaitGroup

	// whether the notifier is currently listening for notificaitons
	listening bool
	// used to cancel listening for notifications
	cancel context.CancelFunc
	// events are sent to this channel when listening has stopped
	stoppedListen chan struct{}
}

func New(ctx context.Context, conn *pgx.Conn, log *slog.Logger) *Notifier {
	// ctxListen, cancel := context.WithCancel(ctx)
	return &Notifier{
		ctx:           ctx,
		conn:          conn,
		log:           log,
		subscribers:   make(map[Topic][]*Subscription),
		stoppedListen: make(chan struct{}, 1),
	}
}

// Creates a new Subscription to topic. The channel size is 100, unless size
// is prvodided.
//
// Once a notification on the topic is received, the payload is sent to the
// channel. If the channel is full when a notification is sent to the subscription,
// the channel will not receive the notification.
func (n *Notifier) Subscribe(topic Topic, size ...int) (*Subscription, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.cmdf("LISTEN %s", topic); err != nil {
		return nil, err
	}

	if !n.listening {
		n.wg.Add(1)
		go n.run()
		n.wg.Wait()
	}

	id := n.seq
	n.seq++

	// by default the channel size is 100, but can be overriden
	size_ := 100
	if len(size) > 0 {
		size_ = size[0]
	}

	s := &Subscription{
		Topic: topic,
		ch:    make(chan string, size_),
		id:    id,
		unsub: func() error { return n.unsubscribe(topic, id) },
	}
	if _, ok := n.subscribers[topic]; !ok {
		n.subscribers[topic] = []*Subscription{}
	}
	n.subscribers[topic] = append(n.subscribers[topic], s)

	return s, nil
}

func (n *Notifier) unsubscribe(topic Topic, id int) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if subs, ok := n.subscribers[topic]; ok {
		del := func(s *Subscription) bool { return s.id == id }
		n.subscribers[topic] = slices.DeleteFunc(subs, del)

		if len(n.subscribers[topic]) > 0 {
			// there are more subscribers, so we still need to listen
			// to the topic.
			return nil
		}

		delete(n.subscribers, topic)

		// No more subscribers for this topic, let's stop listening
		if err := n.cmdf("UNLISTEN %s", topic); err != nil {
			return err
		}

		if len(n.subscribers) == 0 {
			// ... and if we have no more topics to listen to, we might as well stop
			// listening altogether
			n.cancel()
		}
	}
	return nil
}

// Executes a command on the connection.
func (n *Notifier) cmdf(format string, a ...any) error {

	// If we're using the connection to listen for notifications, we need to
	// temporarily stop listening so we can use the connection to send the
	// command. After the command is sent, we start listening again.
	if n.listening {
		n.cancel()
		<-n.stoppedListen
		defer func() {
			// we did listen before, so we start again. Make sure we only
			// start in case there are subscribers.
			// Not my proudest lines of code..
			if len(n.subscribers) > 0 {
				n.wg.Add(1)
				go n.run()
				n.wg.Wait()

			}
		}()
	}

	sql := fmt.Sprintf(format, a...)
	_, err := n.conn.Exec(n.ctx, sql)
	return err
}

// run listens for notifications and relays them to subscribers
func (n *Notifier) run() {
	n.wg.Done()
	if n.listening {
		panic("already listening")
	}
	n.listening = true
	ctx, cancel := context.WithCancel(n.ctx)
	n.cancel = cancel
	defer func() {
		n.log.Info("listening done")
		n.listening = false
		n.stoppedListen <- struct{}{}
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

		topic := Topic(notif.Channel)
		subs, _ := n.subscribers[topic]
		for _, s := range subs {
			n.log.Info("sending notification", "count", len(subs), "payload", notif.Payload)

			// Drop messages in case the subscriber channel is full
			select {
			case s.ch <- notif.Payload:
			default:
			}
		}
	}
}

func (n *Notifier) Close() error {
	n.conn.Close(n.ctx)
	n.cancel()
	if n.listening {
		<-n.stoppedListen
	}
	close(n.stoppedListen)
	return nil
}
