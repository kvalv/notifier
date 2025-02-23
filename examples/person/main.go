package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kvalv/notifier-mvp"
	"github.com/kvalv/notifier-mvp/examples/person/generated"
)

func main() {
	dsn := "postgres://postgres:postgres@localhost:5432/notifier_mvp"
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}

	listenConn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}

	nopLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	notif := notifier.New(ctx, listenConn.Conn(), nopLog)
	sub, err := notif.Subscribe("person")
	if err != nil {
		panic(err)
	}
	go func() {
		for id := range sub.Channel() {
			log.Info("Person event", "id", id)
		}
	}()

	for i := 0; i < 5; i++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			panic(err)
		}

		if _, err := generated.New(tx).CreatePerson(ctx, generated.CreatePersonParams{
			Name: "John Doe",
			Age:  30,
		}); err != nil {
			panic(err)
		}
		if err := tx.Commit(ctx); err != nil {
			panic(err)
		}
		log.Info("Person created")
		time.Sleep(time.Millisecond * 200)
	}

	log.Info("Done")
}
