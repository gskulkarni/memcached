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
  Magic     uint8  // Magic indicates if it is request or response.
  Opcode    uint8  // Opcode to indicate command type SET/GET/GETK....etc.
  KeyLen    uint16 // length of key part of payload
  ExtrasLen uint8  // length of extra content which is command specific.
  DataType  uint8  // reserved
  Reserved  uint16 // reserved
  BodyLen   uint32 // payload length
  Opaque    uint32 // will be relayed back in response as is
  CAS       uint64 // CAS for data version check
}

// read unmarshals content from reader in to RequestHeader.
func (hdr *RequestHeader) read(r io.Reader) error {
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

// Constants for different commands. These are basically values of Opcode
// in the request/response headers.
const (
  CmdGet = 0x00
  CmdSet = 0x01
  // and rest of the codes to follow here...
)

// Command defines the capability of a command implementation. Each command has
// to implement the following set of methods.
type Command interface {
  // Read decodes the content from reader in to Command.
  Read(io.Reader) error

  // IsValid validates the command.
  IsValid() (bool, string)

  // Execute handles the execution of the command.
  Execute() (CommandRspWriter, error)
}

// CommandRspWriter defines capability for a command response writer.
type CommandRspWriter interface {
  Write(io.Writer) error
}
