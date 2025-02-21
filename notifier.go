package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type topic = string

type Notifier struct {
	ctx    context.Context
	cancel context.CancelFunc
	db     *pgxpool.Pool
	conn   *pgxpool.Conn
	// conn   *pgx.Conn
	subs map[topic][]*Subscription
	mu   sync.Mutex
	log  *slog.Logger
}

func NewNotifier(ctx context.Context, db *pgxpool.Pool, log *slog.Logger) *Notifier {
	ctx, cancel := context.WithCancel(ctx)
	n := &Notifier{
		ctx:    ctx,
		cancel: cancel,
		subs:   make(map[topic][]*Subscription),
		db:     db,
		// conn:   conn,
		log: log,
	}
	go n.listen()
	return n
}

func (n *Notifier) Subscribe(topic string, callback Callback) (*Subscription, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.execListen(topic); err != nil {
		return nil, err
	}

	s := &Subscription{cb: callback}
	n.addSubscriber(topic, s)

	return s, nil
}

func (n *Notifier) addSubscriber(topic topic, s *Subscription) {
	if _, ok := n.subs[topic]; !ok {
		n.subs[topic] = []*Subscription{}
	}
	n.subs[topic] = append(n.subs[topic], s)
}

func (n *Notifier) execListen(topic string) error {
	conn, err := n.db.Acquire(n.ctx)
	if err != nil {
		return err
	}
	n.conn = conn
	if _, err := conn.Conn().Exec(n.ctx, fmt.Sprintf("listen %s", topic)); err != nil {
		return err
	}
	n.log.Info("subscribed to topic", "topic", topic)
	return nil
}

func (n *Notifier) listen() {
	time.Sleep(10 * time.Millisecond)
	conn := n.conn
	if conn == nil {
		n.log.Error("conn is nil")
		return
	}
	for {
		n.log.Info("waiting for notification")
		notif, err := conn.Conn().WaitForNotification(n.ctx)
		if err != nil {
			n.log.Error("failed to get conn", "err", err)
			return
		}
		n.log.Info("received notification", "channel", notif.Channel, "payload", notif.Payload)

		subs, _ := n.subs[notif.Channel]
		for _, s := range subs {
			// send to each subscriber
			s.cb(notif.Payload)
		}
	}
}

func (n *Notifier) Close() error {
	n.cancel()
	return nil
}
