package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func randomTopic() Topic {
	return Topic(fmt.Sprintf("topic_%d", time.Now().UnixNano()))
}

func TestNotifier(t *testing.T) {
	dsn := "postgres://postgres:postgres@localhost:5432/notifier_mvp"
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	t.Run("single topic", func(t *testing.T) {
		notifier := mustNewNotifier(t, pool, log)
		topic := randomTopic()
		sub, err := notifier.Subscribe(topic)
		if err != nil {
			t.Fatalf("failed to sub: %s", err)
		}

		var count int
		go func() {
			for range sub.Channel() { // consume until closed
				count++
			}
		}()

		notify(t, pool, topic, "hello")
		notify(t, pool, topic, "hello")

		time.Sleep(1 * time.Millisecond)
		sub.Close()
		time.Sleep(1 * time.Millisecond)

		if want := 2; count != want {
			t.Fatalf("notification count mismatch; want=%d, got=%d", want, count)
		}
	})

	t.Run("multiple topics", func(t *testing.T) {
		// t.Skip()
		notifier := mustNewNotifier(t, pool, log)
		topic1, topic2 := randomTopic(), randomTopic()
		s1, err := notifier.Subscribe(topic1)
		if err != nil {
			t.Fatalf("failed to sub: %s", err)
		}
		s2, err := notifier.Subscribe(topic2)
		if err != nil {
			t.Fatalf("failed to sub: %s", err)
		}
		t.Logf("subscribers set up")

		var count1, count2 int
		go func() {
			for range s1.Channel() {
				count1++
			}
		}()
		go func() {
			for range s2.Channel() {
				count2++
			}
		}()

		notify(t, pool, topic1, "y")
		notify(t, pool, topic2, "x")
		time.Sleep(1 * time.Millisecond)
		s1.Close()
		s2.Close()
		time.Sleep(1 * time.Millisecond)

		if want := 1; count1 != want {
			t.Fatalf("notification count mismatch; want=%d, got=%d", want, count1)
		}
		if want := 1; count2 != want {
			t.Fatalf("notification count mismatch; want=%d, got=%d", want, count2)
		}
	})

}

func notify(t *testing.T, pool *pgxpool.Pool, topic Topic, msg string) {
	sql := fmt.Sprintf("select pg_notify('%s', '%s')", topic, msg)
	_, err := pool.Exec(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
}

// creates a new pool and returns a new notifier
func mustNewNotifier(t *testing.T, pool *pgxpool.Pool, log *slog.Logger) *Notifier {
	t.Helper()
	ctx := context.Background()
	tmp, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire connection: %s", err)
	}
	conn := tmp.Hijack()
	return NewNotifier(ctx, conn, log)
}
