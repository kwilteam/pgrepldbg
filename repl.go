package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func connString(host, port, user, pass, dbName string, repl bool) string {
	connStr := fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable",
		host, user, dbName)

	if pass != "" {
		connStr += fmt.Sprintf(" password=%s", pass)
	}

	// Only add port for TCP connections since UNIX domain sockets.
	if !strings.HasPrefix(host, "/") {
		connStr += fmt.Sprintf(" port=%s", port)
	}

	if repl {
		connStr += " replication=database"
	}

	return connStr
}

func replConn(ctx context.Context, host, port, user, pass, dbName string) (*pgconn.PgConn, error) {
	const repl = true
	connStr := connString(host, port, user, pass, dbName, repl)

	return pgconn.Connect(ctx, connStr)
}

// Must of the decoding logic in the following methods is based on the example
// code from github.com/jackc/pglogrepl

var typeMap = pgtype.NewMap()

func startRepl(ctx context.Context, conn *pgconn.PgConn, publicationName, slotName string) (chan []byte, chan error, error) {
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return nil, nil, err
	}

	log.Println("DBName:", sysident.DBName, "XLogPos:", sysident.XLogPos,
		"SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline)

	// const publicationName = "kwild_repl"
	// Creating the publication should be done with psql as a superuser when
	// creating the kwild database and role.
	//  e.g.
	//  CREATE USER kwild WITH REPLICATION; -- verify: SELECT rolname, rolreplication FROM pg_roles WHERE rolreplication = true;
	//  CREATE DATABASE kwild OWNER kwild;
	//  -- then '\c kwild' to connect to the kwild database
	//  CREATE PUBLICATION kwild_repl FOR ALL TABLES; -- applies to connected DB!

	pubRes := conn.Exec(ctx, fmt.Sprintf(`SELECT * FROM pg_publication WHERE pubname = '%v';`, publicationName))
	if qr, err := pubRes.ReadAll(); err != nil {
		return nil, nil, err
	} else if len(qr) == 0 {
		return nil, nil, fmt.Errorf("publication does not exist: %v", publicationName)
	}

	const outputPlugin = "pgoutput" // built-in
	// const slotName = "kwild_repl"
	slotRes, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Mode:      pglogrepl.LogicalReplication,
			Temporary: true,
		})
	if err != nil {
		return nil, nil, err
	}
	slotLSN, _ := pglogrepl.ParseLSN(slotRes.ConsistentPoint)
	log.Printf("Created logical replication slot %v at LSN %v (%d)\n",
		slotRes.SlotName, slotRes.ConsistentPoint, slotLSN)

	pluginArgs := []string{
		"proto_version '2'",
		"publication_names '" + publicationName + "'",
		"messages 'true'",
		"streaming 'true'",
	}
	err = pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
			Mode:       pglogrepl.LogicalReplication,
		})
	if err != nil {
		return nil, nil, fmt.Errorf("StartReplication failed: %w", err)
	}

	done := make(chan error, 1)
	commitHash := make(chan []byte, 1)

	go func() {
		defer close(done)
		defer close(commitHash)

		clientXLogPos := sysident.XLogPos
		standbyMessageTimeout := time.Second * 10
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
		hasher := sha256.New()
		relations := map[uint32]*pglogrepl.RelationMessageV2{}

		var inStream bool

		for {
			if ctx.Err() != nil {
				done <- ctx.Err()
				return
			}
			if time.Now().After(nextStandbyMessageDeadline) {
				err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
				if err != nil {
					done <- fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
					return
				}
				// fmt.Printf("Sent Standby status message at %s (%d)\n", clientXLogPos.String(), uint64(clientXLogPos))
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

			ctxStandby, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
			rawMsg, err := conn.ReceiveMessage(ctxStandby)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue // nextStandbyMessageDeadline hit
				}
				done <- fmt.Errorf("ReceiveMessage failed: %w", err)
				return
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				done <- fmt.Errorf("received Postgres WAL error: %+v", errMsg)
				return
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				log.Printf("Received unexpected message: %T\n", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					done <- fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
					return
				}
				// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd,
				// 	"ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					done <- fmt.Errorf("ParseXLogData failed: %w", err)
					return
				}

				commit, err := decodeWALData(hasher, xld.WALData, relations, &inStream)
				if err != nil {
					done <- fmt.Errorf("decodeWALData failed: %w", err)
					return
				}

				var lsnDelta uint64
				if xld.WALStart > clientXLogPos {
					lsnDelta = uint64(xld.WALStart - clientXLogPos)
					clientXLogPos = xld.WALStart
				}

				// fmt.Printf("XLogData (in stream? %v) => WALStart %s ServerWALEnd %s\n",
				// 	inStream, xld.WALStart, xld.ServerWALEnd)

				if commit {
					cHash := hasher.Sum(nil)
					commitHash <- cHash
					hasher.Reset() // hasher = sha256.New()
					log.Printf("Commit id %x, LSN %v (%d) delta %d\n", cHash[:6], xld.WALStart, xld.WALStart, lsnDelta)
				}
			}
		}

	}()

	return commitHash, done, nil
}

func mustMarshal(vals map[string]any) string {
	b, _ := json.Marshal(vals)
	return string(b)
}

func decodeWALData(hasher hash.Hash, walData []byte, relations map[uint32]*pglogrepl.RelationMessageV2,
	inStream *bool) (bool, error) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		return false, fmt.Errorf("Parse logical replication message: %w", err)
	}

	// log.Printf("### Receive a logical replication message: <%s>\n", logicalMsg.Type())

	var done bool // set to true on receipt of a commit to signal the the end of a transaction

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		log.Printf(" [msg] Relation: %d (%v.%v)\n", logicalMsg.RelationID,
			logicalMsg.Namespace, logicalMsg.RelationName)
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		log.Printf(" [msg] BEGIN: LSN %v (%d)\n", logicalMsg.FinalLSN, uint64(logicalMsg.FinalLSN))
		// Indicates the beginning of a group of changes in a transaction. This
		// is only sent for committed transactions. You won't get any events
		// from rolled back transactions.

	case *pglogrepl.CommitMessage:
		log.Printf(" [msg] COMMIT: Commit LSN %v (%d), End LSN %v (%d) \n",
			logicalMsg.CommitLSN, uint64(logicalMsg.CommitLSN),
			logicalMsg.TransactionEndLSN, uint64(logicalMsg.TransactionEndLSN))

		done = true

	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return false, fmt.Errorf("insert: unknown relation ID %d", logicalMsg.RelationID)
		}

		relName := rel.Namespace + "." + rel.RelationName
		insertData := encodeInsertMsg(relName, &logicalMsg.InsertMessage)
		log.Printf("insertData %x", insertData)
		hasher.Write(insertData)

		values, err := tuplColVals(logicalMsg.Tuple.Columns, rel)
		if err != nil {
			return false, err
		}

		log.Printf(" [msg] INSERT into %v.%v: %v\n", rel.Namespace, rel.RelationName, mustMarshal(values))

	case *pglogrepl.UpdateMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return false, fmt.Errorf("insert: unknown relation ID %d", logicalMsg.RelationID)
		}

		var oldValues map[string]any
		if logicalMsg.OldTuple != nil { // seems to be only if primary key changes
			oldValues, err = tuplColVals(logicalMsg.OldTuple.Columns, rel)
			if err != nil {
				return false, err
			}
		}
		newValues, err := tuplColVals(logicalMsg.NewTuple.Columns, rel)
		if err != nil {
			return false, err
		}

		log.Printf(" [msg] UPDATE rel %v.%v: %v (%v) => %v\n", rel.Namespace, rel.RelationName,
			mustMarshal(oldValues), rune(logicalMsg.OldTupleType), mustMarshal(newValues))

		relName := rel.Namespace + "." + rel.RelationName
		updateData := encodeUpdateMsg(relName, &logicalMsg.UpdateMessage)
		hasher.Write(updateData)

	case *pglogrepl.DeleteMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			return false, fmt.Errorf("insert: unknown relation ID %d", logicalMsg.RelationID)
		}

		oldValues, err := tuplColVals(logicalMsg.OldTuple.Columns, rel)
		if err != nil {
			return false, err
		}

		log.Printf(" [msg] DELETE from rel %v.%v: %v\n", rel.Namespace, rel.RelationName, mustMarshal(oldValues))

		relName := rel.Namespace + "." + rel.RelationName
		deleteData := encodeDeleteMsg(relName, &logicalMsg.DeleteMessage)
		hasher.Write(deleteData)

	case *pglogrepl.TruncateMessageV2:
		log.Printf(" [msg] TRUNCATE relations: %v\n", logicalMsg.RelationIDs)

		hasher.Write(encodeTruncateMsg(&logicalMsg.TruncateMessage))

	case *pglogrepl.TypeMessageV2:
		log.Println(" [msg] type message", logicalMsg.Name, logicalMsg.Namespace, logicalMsg.DataType)
	case *pglogrepl.OriginMessage:
		log.Println(" [msg] origin message", logicalMsg.Name, logicalMsg.CommitLSN)
	case *pglogrepl.LogicalDecodingMessageV2:
		log.Printf(" [msg] logical decoding message: %q, %q, %d\n", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	// Stream messages.  Not expected for kwil
	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		log.Printf(" [msg] StreamStartMessageV2: xid %d, first segment? %d\n", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		log.Println(" [msg] StreamStopMessageV2")
	case *pglogrepl.StreamCommitMessageV2:
		log.Printf("Stream commit message: xid %d\n", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d\n", logicalMsg.Xid)

	default:
		log.Printf("Unknown message type in pgoutput stream: %T\n", logicalMsg)
	}

	return done, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func tuplColVals(cols []*pglogrepl.TupleDataColumn, rel *pglogrepl.RelationMessageV2) (map[string]any, error) {
	values := map[string]any{}
	for idx, col := range cols {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored
			// in the tuple, and logical replication doesn't want to spend a
			// disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("error decoding column data: %w", err)
			}
			values[colName] = val
		}
	}
	return values, nil
}

var pgIntCoder = binary.BigEndian

func encodeTupleData(td *pglogrepl.TupleData) []byte {
	if td == nil {
		return []byte{0}
	}
	var data []byte
	data = pgIntCoder.AppendUint16(data, td.ColumnNum)
	for _, col := range td.Columns {
		data = append(data, col.DataType)

		switch col.DataType {
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			pgIntCoder.AppendUint32(data, col.Length) // len(col.Data)
			data = append(data, col.Data...)
		case pglogrepl.TupleDataTypeNull, pglogrepl.TupleDataTypeToast:
		}
	}
	return data
}

func encodeInsertMsg(relName string, im *pglogrepl.InsertMessage) []byte {
	data := []byte(relName) // RelationID is dependent on the deployment
	return append(data, encodeTupleData(im.Tuple)...)
}

func encodeUpdateMsg(relName string, um *pglogrepl.UpdateMessage) []byte {
	data := []byte(relName) // RelationID is dependent on the deployment
	data = append(data, um.OldTupleType)
	data = append(data, encodeTupleData(um.OldTuple)...)
	return append(data, encodeTupleData(um.NewTuple)...)
}

func encodeDeleteMsg(relName string, um *pglogrepl.DeleteMessage) []byte {
	data := []byte(relName) // RelationID is dependent on the deployment
	data = append(data, um.OldTupleType)
	return append(data, encodeTupleData(um.OldTuple)...)
}

func encodeTruncateMsg(tm *pglogrepl.TruncateMessage) []byte {
	data := pgIntCoder.AppendUint32(nil, tm.RelationNum)
	data = append(data, tm.Option)
	for _, rid := range tm.RelationIDs {
		data = pgIntCoder.AppendUint32(data, rid)
	}
	return data
}
