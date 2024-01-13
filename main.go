package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-signalChan
		cancel()
	}()

	if err := mainCore(ctx); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

var (
	host, port, user, pass, dbName string
	dur                            time.Duration
	publicationName                string
)

func mainCore(ctx context.Context) error {
	flag.StringVar(&host, "host", "/var/run/postgresql", "pg host (ip or unix socket path)")
	flag.StringVar(&port, "port", "5432", "pg port (ignored for unix socket)")
	flag.StringVar(&user, "user", "kwild", "pg user")
	flag.StringVar(&pass, "pass", "", "pg pass (may be empty depending on pg_hba.conf)")
	flag.StringVar(&dbName, "db", "kwild", "pg database name")
	flag.DurationVar(&dur, "duration", time.Hour, "duration to run")
	flag.StringVar(&publicationName, "publication", "kwild_repl", "publication name created by DB admin with CREATE PUBLICATION {name} FOR ALL TABLES")
	flag.Parse()

	// host, port, user, pass, dbName = "/var/run/postgresql", "", "kwil_test_user", "kwilsekrit", "kwil_test_db"

	// Create the replication connection.
	conn, err := replConn(ctx, host, port, user, pass, dbName)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Start listening and printing messages on a new replication slot.
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(dur))
	defer cancel()

	const slotName = "kwild_repl"
	commitChan, errChan, err := startRepl(ctx, conn, publicationName, slotName)
	if err != nil {
		return err
	}

	log.Printf("Listening for WAL messages on replication slot %q\n", publicationName)

	for {
		select {
		case commitHash := <-commitChan:
			log.Printf("Commit HASH: %x\n", commitHash)
		case err = <-errChan: // wait for startRepl's goroutine complete
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
	}
}
