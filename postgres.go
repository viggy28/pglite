package main

import "encoding/json"

type FieldDescription struct {
	Name                 []byte
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
	DataTypeSize         int16
	TypeModifier         int32
	Format               int16
}

type MessageType byte

const (
	MessageTypeTerminate   MessageType = 'X'
	MessageTypeSimpleQuery MessageType = 'Q'
)

type StartupMessageType uint32

const (
	SSLNegotiationStartupMessageType StartupMessageType = 80877103
	StartupMessage                   StartupMessageType = 196608
)

// MarshalJSON implements encoding/json.Marshaler.
func (fd FieldDescription) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name                 string
		TableOID             uint32
		TableAttributeNumber uint16
		DataTypeOID          uint32
		DataTypeSize         int16
		TypeModifier         int32
		Format               int16
	}{
		Name:                 string(fd.Name),
		TableOID:             fd.TableOID,
		TableAttributeNumber: fd.TableAttributeNumber,
		DataTypeOID:          fd.DataTypeOID,
		DataTypeSize:         fd.DataTypeSize,
		TypeModifier:         fd.TypeModifier,
		Format:               fd.Format,
	})
}

type QueryType string

const (
	QueryTypeSelect      QueryType = "select"
	QueryTypeInsert      QueryType = "insert"
	QueryTypeUpdate      QueryType = "update"
	QueryTypeDelete      QueryType = "delete"
	QueryTypeCreateTable QueryType = "create table"
	QueryTypeInvalid     QueryType = "invalid"
)
