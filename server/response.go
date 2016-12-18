package server

import (
  "encoding/binary"
  "io"
)

// constants for Status code for response.
const (
  StatusNoError       = 0x0000
  StatusKeyNotFound   = 0x0001
  StatusKeyExists     = 0x0002
  StatusValueTooLarge = 0x0003
  StatusInvalidArgs   = 0x0004
  StatusItemNotStored = 0x0005
  StatusInvalidOp     = 0x0006
  StatusUnknownCmd    = 0x0081
  StatusOutOfMemory   = 0x0082
)

type ResponseHeader struct {
  Magic     uint8
  Opcode    uint8
  KeyLen    uint16
  ExtrasLen uint8
  DataType  uint8
  Status    uint16
  BodyLen   uint32
  Opaque    uint32
  CAS       uint64
}

func (hdr *ResponseHeader) encode(w io.Writer) error {
  hdrFields := []interface{}{
    &hdr.Magic, &hdr.Opcode, &hdr.KeyLen, &hdr.ExtrasLen, &hdr.DataType,
    &hdr.Status, &hdr.BodyLen, &hdr.Opaque, &hdr.CAS,
  }
  return writeFields(w, hdrFields...)
}

func writeFields(w io.Writer, fields ...interface{}) error {
  for _, f := range fields {
    if err := binary.Write(w, binary.BigEndian, f); err != nil {
      return err
    }
  }
  return nil
}
