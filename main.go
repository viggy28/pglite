package main

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/jackc/pgproto3"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, close := validate()
	defer close()
	var dataDir string
	flag.StringVar(&dataDir, "datadir", "", "provide the absolute path to the sqlite database")
	flag.Parse()
	InfoLogger.Println("data directory for sqlite db is ", dataDir)

	listenAddr := net.TCPAddr{
		Port: 5432,
	}
	listener, err := net.ListenTCP("tcp", &listenAddr)
	if err != nil {
		log.Fatalf("error while trying to start the server %v", err)
	}
	InfoLogger.Println("info: server listening on ", listener.Addr().String())

	// To continously accept multiple connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			ErrorLogger.Printf("error accepting connection from %s %s %v", conn.RemoteAddr().Network(), conn.RemoteAddr().String(), err)
			continue
		}
		InfoLogger.Println("received a new connection from ", conn.RemoteAddr().Network(), conn.RemoteAddr().String())
		// handle the client connection in a separate go routine so we don't block subsequent client connections
		go handler(db, conn)
	}
}

// handler handles the client connection. It doesn't have any return value
// However, when the function returns because of any error, it closes the client connection
// called using defer
func handler(db *sql.DB, c net.Conn) {
	defer func() {
		InfoLogger.Println("closing client connection", c.RemoteAddr().Network(), c.RemoteAddr().String())
		c.Close()
	}()

	if err := handleStartup(c); err != nil {
		InfoLogger.Printf("error handling startup bytes: %v \n", err)
		return
	}
	for {
		readBuf := make([]byte, 5)
		// Read() blocks until it can read from conn
		_, err := c.Read(readBuf)
		if err != nil {
			ErrorLogger.Printf("unable to read data from buffer %v \n", err)
			return
		}
		// byte ordering
		// message-type		message-length		message
		// []  				[][][][] 			[][][][][][]....
		// Note: message-length count includes itself, but not the message-type byte
		// Note: The very first message sent by the client (the startup message) has no initial message-type byte.
		// Rest of every other message has the first byte as message-type byte
		msgType := readBuf[0]
		if msgType == byte(MessageTypeTerminate) {
			return
		}
		// Subtact 4 since that's the number of bytes it takes to store an integer
		// i.e. remove the number of bytes it takes to carry the message itself
		msgLen := binary.BigEndian.Uint32(readBuf[1:5]) - 4
		queryByte := make([]byte, msgLen)
		if _, err = c.Read(queryByte); err != nil {
			ErrorLogger.Printf("unable to read query %v \n", err)
			return
		}
		query := string(queryByte)
		InfoLogger.Println("received query:", query)
		switch parseQueryToQueryType(query) {
		case QueryTypeCreateTable:
			_, err := ExecSqlite(db, query)
			if err != nil {
				ErrorLogger.Printf("unable to execute create table query %v", err)
				errorResponse(c, "check your query", err)
				break
			}
			cc := pgproto3.CommandComplete{CommandTag: "CREATE TABLE"}
			buf := cc.Encode(nil)
			writeResponse(c, buf)
		case QueryTypeSelect:
			//TODO: merge prepareRowDescription and prepareDataRow into a single function
			// to avoid sending the same query twice to SQLite
			description, err := prepareRowDescription(db, query)
			if err != nil {
				ErrorLogger.Printf("unable to prepare row description %v", err)
				errorResponse(c, "check your query", err)
				break
			}
			buf := (&pgproto3.RowDescription{
				Fields: description,
			}).Encode(nil)

			resultSet, err := prepareDataRow(db, query)
			if err != nil {
				ErrorLogger.Printf("unable to prepare data row %v", err)
				errorResponse(c, "check your query", err)
				break
			}
			for _, row := range resultSet {
				buf = (&pgproto3.DataRow{
					Values: row,
				}).Encode(buf)
			}
			// FIG: what's supposed to be in CommandTag value. I can pass in anything
			// It's supposed to be the number of rows returned by SELECT
			buf = (&pgproto3.CommandComplete{CommandTag: ""}).Encode(buf)
			writeResponse(c, buf)
		case QueryTypeInsert:
			rowsAffected, err := ExecSqlite(db, query)
			if err != nil {
				ErrorLogger.Printf("unable to execute insert query %v", err)
				errorResponse(c, "check your query", err)
				return
			}
			cc := pgproto3.CommandComplete{CommandTag: "INSERT 0 " + fmt.Sprintf("%d", rowsAffected)}
			buf := cc.Encode(nil)
			writeResponse(c, buf)
		case QueryTypeUpdate:
			rowsAffected, err := ExecSqlite(db, query)
			if err != nil {
				ErrorLogger.Printf("unable to execute update query %v", err)
				errorResponse(c, "check your query", err)
				return
			}
			cc := pgproto3.CommandComplete{CommandTag: "UPDATE " + fmt.Sprintf("%d", rowsAffected)}
			buf := cc.Encode(nil)
			writeResponse(c, buf)
		case QueryTypeDelete:
			rowsAffected, err := ExecSqlite(db, query)
			if err != nil {
				ErrorLogger.Printf("unable to execute delete query %v", err)
				errorResponse(c, "check your query", err)
				return
			}
			cc := pgproto3.CommandComplete{CommandTag: "DELETE " + fmt.Sprintf("%d", rowsAffected)}
			buf := cc.Encode(nil)
			writeResponse(c, buf)
		default:
			InfoLogger.Println("unsupported query type")
			errorResponse(c, "", errors.New("unsupported query type"))
		}
	}
}

func handleStartup(c net.Conn) error {
	InfoLogger.Println("handle startup for client connection", c.RemoteAddr().Network(), c.RemoteAddr().String())

	// byte ordering
	// message-length		message
	// [][][][] 			[][][][][][]....
	// message-length count includes itself. i.e. if the content of message-length is [0 0 0 84]
	// subtract 4 from it (i.e. subtract 4 bytes from the message-length bytes value) and the rest 80 bytes
	// is the actual message

	readBuf := make([]byte, 4)
	_, err := c.Read(readBuf)
	if err != nil {
		return fmt.Errorf("unable to read data from buffer %v", err)
	}
	// convert (aka decode) data stored in bytes to integer
	msgSize := binary.BigEndian.Uint32(readBuf) - 4
	InfoLogger.Printf("startup message size including message-length: %d", int(binary.BigEndian.Uint32(readBuf)))
	if msgSize < 4 || msgSize > 10000 {
		return fmt.Errorf("invalid length of startup message size: %d", msgSize)
	}
	msgBuf := make([]byte, msgSize)
	_, err = c.Read(msgBuf)
	if err != nil {
		return fmt.Errorf("unable to read data from message buffer %v", err)
	}
	// InfoLogger.Println("read message ", string(msgBuf))
	message := binary.BigEndian.Uint32(msgBuf)

	// InfoLogger.Printf("message in bytes %d \n", msgBuf)
	// InfoLogger.Printf("message in integer %d \n", message)

	// An example message format
	// message size including message-length: 8
	// complete message: [0][0][0][8] [4][210][22][47]
	// message in bytes [4 210 22 47]
	// message in integer 80877103

	// That magic number 80877103 represents SSLRequest
	// https://www.postgresql.org/docs/current/protocol-message-formats.html

	switch message {
	case uint32(SSLNegotiationStartupMessageType):
		// SSL Negotiation phase
		c.Write([]byte("N"))
		return handleStartup(c)
	case uint32(StartupMessage):
		// handling startup message is done without any external package

		// message format looks like below for AuthenticationOk(B)
		// ['R'] [][][][8] [][][][0]
		c.Write([]byte("R"))
		messageLen := 8
		lenByte := make([]byte, 4)
		binary.BigEndian.PutUint32(lenByte, uint32(messageLen))
		if _, err := c.Write(lenByte); err != nil {
			return fmt.Errorf("error writing lenByte %v in AuthenticationOk", err)
		}
		successfulAuth := 0
		authByte := make([]byte, 4)
		binary.BigEndian.PutUint32(authByte, uint32(successfulAuth))
		if _, err := c.Write(authByte); err != nil {
			return fmt.Errorf("unable to write auth byte")
		}

		// message format looks like below for a BackendKeyData(B)
		// ['K'] [][][][12] [0][0][4][210] [0][0][22][46]
		// processId 1234 and secretKey 5678 are picked in random
		c.Write([]byte("K"))
		messageLen = 12
		lenByte = make([]byte, 4)
		binary.BigEndian.PutUint32(lenByte, uint32(messageLen))
		if _, err := c.Write(lenByte); err != nil {
			return fmt.Errorf("error writing lenByte in BackendKeyData %v", err)
		}
		processId := 1234
		processIdByte := make([]byte, 4)
		binary.BigEndian.PutUint32(processIdByte, uint32(processId))
		if _, err := c.Write(processIdByte); err != nil {
			return fmt.Errorf("error writing processIdByte %v", err)
		}
		secretKey := 5678
		secretKeyByte := make([]byte, 4)
		binary.BigEndian.PutUint32(secretKeyByte, uint32(secretKey))
		if _, err := c.Write(processIdByte); err != nil {
			return fmt.Errorf("error writing secretKeyByte %v", err)
		}

		// message format looks like below for a ReadyForQuery(B)
		// ['Z'] [][][][5] [I]
		// I represents the connection is Idle and ready for a query
		c.Write([]byte("Z"))
		messageLen = 5
		lenByte = make([]byte, 4)
		binary.BigEndian.PutUint32(lenByte, uint32(messageLen))
		if _, err = c.Write(lenByte); err != nil {
			return fmt.Errorf("error writing lenByte in ReadyForQuery %v", err)
		}
		if _, err = c.Write([]byte("I")); err != nil {
			return fmt.Errorf("error writing Idle byte in ReadyForQuery %v", err)
		}
	}
	return nil
}
