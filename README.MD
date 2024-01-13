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

## Example

```
$ ./pgrepldbg -user kwil_test_user -db kwil_test_db
2024/01/13 14:01:53 DBName: kwil_test_db XLogPos: 475/23A2C470 SystemID: 7310065834048781943 Timeline: 1
2024/01/13 14:01:53 Created logical replication slot kwild_repl at LSN 475/23A2C4A8 (4901155554472)
2024/01/13 14:01:53 Listening for WAL messages on replication slot "kwild_repl"
2024/01/13 14:02:03  [msg] BEGIN: LSN 475/23A2C598 (4901155554712)
2024/01/13 14:02:03  [msg] Relation: 8838829 (public.boring)
2024/01/13 14:02:03  [msg] INSERT into public.boring: {"id":"ezExfQ==","stuff":"blah","val":42}
2024/01/13 14:02:03  [msg] COMMIT: Commit LSN 475/23A2C598 (4901155554712), End LSN 475/23A2C5C8 (4901155554760) 
2024/01/13 14:02:03 Commit id 8f6659b0ba16, LSN 475/23A2C5C8 (4901155554760) delta 288
2024/01/13 14:02:03 Commit HASH: 8f6659b0ba167547283181f23a2d35f5a55dd1c297842ad99abc4d86d38af894
```

The above is logged for the interaction with `psql`:

```
psql://postgres@[local]:5432/kwil_test_db 
# insert INTO boring values ( '{11}', 'blah' , 42);
INSERT 0 1
Time: 0.411 ms
```

## License

[MIT](https://mit-license.org/)
