# pgReplDBG

This tool creates a replication slot on a PostgreSQL host and listens for
messages that pertain to data modification. It prints the modifications and
logs a digest of the modifications.

The digest is a SHA256 hash of all the "WAL data" bytes from the following
messages types: INSERT, UPDATE, DELETE, TRUNCATE.

## Use

```bash
git clone https://github.com/kwilteam/pgrepldbg
go build
./pgrepldbg
```

See the output of `pgrepldbg -h` for details on the options, which are:

```text
Usage of pgrepldbg:
  -db string
        pg database name (default "kwild")
  -duration duration
        duration to run (default 1h0m0s)
  -host string
        pg host (ip or unix socket path) (default "/var/run/postgresql")
  -pass string
        pg pass (may be empty depending on pg_hba.conf)
  -port string
        pg port (ignored for unix socket) (default "5432")
  -publication string
        publication name created by DB admin with CREATE PUBLICATION {name} FOR ALL TABLES (default "kwild_repl")
  -user string
        pg user (default "kwild")
```

## License

[MIT](https://mit-license.org/)
