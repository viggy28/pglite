package main

import (
	"database/sql"
	"net"
	"regexp"

	"github.com/jackc/pgproto3"
)

type pgDataType struct {
	DataTypeOID  int
	DataTypeSize int
}

// makeDataTypeLookup returns a lookup of pgDataType for specific sqlite types
// To find the OID and Size query pg_catalog.pg_type oid and typlen values;
// TODO: Add more keys to support more types

func makeDataTypeLookup() map[string]pgDataType {
	return map[string]pgDataType{
		"INT": {
			DataTypeOID:  23,
			DataTypeSize: 4,
		},
		"DECIMAL": {
			DataTypeOID:  700,
			DataTypeSize: 4,
		},
		"TEXT": {
			DataTypeOID:  25,
			DataTypeSize: -1,
		},
	}
}

func prepareRowDescription(db *sql.DB, query string) ([]pgproto3.FieldDescription, error) {
	dataTypeLookup := makeDataTypeLookup()
	rowPair, err := querySqliteRowDescription(db, query)
	if err != nil {
		return nil, err
	}
	var fd []pgproto3.FieldDescription
	for fieldName, fieldType := range rowPair {
		fd = append(fd, pgproto3.FieldDescription{
			Name:                 fieldName,
			TableOID:             0,
			TableAttributeNumber: 0,
			// mapping sqlite types to Postgres' type OID and size
			DataTypeOID:  uint32(dataTypeLookup[fieldType].DataTypeOID),
			DataTypeSize: int16(dataTypeLookup[fieldType].DataTypeSize),
			TypeModifier: -1,
			Format:       0,
		})
	}
	return fd, nil
}

var reForSelect = regexp.MustCompile(`select|SELECT`)
var reForInsert = regexp.MustCompile(`insert|INSERT`)
var reForUpdate = regexp.MustCompile(`update|UPDATE`)
var reForDelete = regexp.MustCompile(`delete|DELETE`)
var reForCreateTable = regexp.MustCompile(`create table|CREATE TABLE`)

func parseQueryToQueryType(query string) QueryType {
	if reForSelect.MatchString(query) {
		return QueryTypeSelect
	} else if reForInsert.MatchString(query) {
		return QueryTypeInsert
	} else if reForUpdate.MatchString(query) {
		return QueryTypeUpdate
	} else if reForDelete.MatchString(query) {
		return QueryTypeDelete
	} else if reForCreateTable.MatchString(query) {
		return QueryTypeCreateTable
	} else {
		return QueryTypeInvalid
	}
}

func writeResponse(c net.Conn, buf []byte) {
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if _, err := c.Write(buf); err != nil {
		ErrorLogger.Printf("unable to write response to client %v", err)
	}
}

func errorResponse(c net.Conn, context string, message error) {
	errRes := pgproto3.ErrorResponse{
		Message: message.Error(),
		Hint:    context,
	}
	buf := errRes.Encode(nil)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	// TODO: handle the error from Write()
	c.Write(buf)
}
