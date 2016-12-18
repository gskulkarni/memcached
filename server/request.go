package server

import (
  "encoding/binary"
  "io"
)

// this file implements Memcached Request object and related functionality

// constants for to indicate if it is Request or Response.
const (
  MagicCodeRequest  = 0x80
  MagicCodeResponse = 0x81
)

type RequestHeader struct {
  Magic     uint8
  Opcode    uint8
  KeyLen    uint16
  ExtrasLen uint8
  DataType  uint8
  Reserved  uint16
  BodyLen   uint32
  Opaque    uint32 // will be relayed back in response as is
  CAS       uint64 // CAS for data version check
}

func (hdr *RequestHeader) decode(r io.Reader) error {
  hdrFields := []interface{}{
    &hdr.Magic, &hdr.Opcode, &hdr.KeyLen, &hdr.ExtrasLen, &hdr.DataType,
    &hdr.Reserved, &hdr.BodyLen, &hdr.Opaque, &hdr.CAS,
  }

  for _, field := range hdrFields {
    if err := binary.Read(r, binary.BigEndian, field); err != nil {
      return err
    }
  }
  return nil
}

const (
  CmdGet = 0x00
  CmdSet = 0x01
  // and rest of the codes....
)

type Command interface {
  Decode(io.Reader) error
  IsValid() (bool, string)
  Execute() (*Response, error)
}
