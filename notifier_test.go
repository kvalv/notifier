package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNotifier(t *testing.T) {
	dsn := "postgres://postgres:postgres@localhost:5432/notifier_mvp"
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	conn := acquireConn(t, ctx, pool)
	defer conn.Close(ctx)

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	notifier := NewNotifier(ctx, pool, log)
	var count int
	callback := func(msg string) {
		count++
	}

	sub, err := notifier.Subscribe("test", callback)
	if err != nil {
		t.Fatalf("failed to sub: %s", err)
	}
	defer sub.Close()

	time.Sleep(25 * time.Millisecond)

	notify(t, pool, "test", "hello")
	notify(t, pool, "test", "hello")

	time.Sleep(25 * time.Millisecond)

	if count != 2 {
		t.Fatalf("notification count mismatch; want=1, got=%d", count)
	}

}

func notify(t *testing.T, pool *pgxpool.Pool, topic, msg string) {
	sql := fmt.Sprintf("select pg_notify('%s', '%s')", topic, msg)
	_, err := pool.Exec(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
}

func acquireConn(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *pgx.Conn {
	t.Helper()
	tmp, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("failed to acquire connection: %s", err)
	}

	return tmp.Hijack()
}
