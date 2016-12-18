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

const (
  CmdGet = 0x00
  CmdSet = 0x01
  // and rest of the codes....

  CmdGetQ = 0x09
  CmdSetQ = 0x11
)

type Command interface {
  Decode(io.Reader) error
  IsValid() (bool, string)
  Execute() (*Response, error)
}

type GetCmd struct {
  s      *Server
  Header *RequestHeader
  Flags  uint32
  Key    []byte
  Value  []byte
}

func (c *GetCmd) Decode(r io.Reader) error {
  var err error
  hdr := c.Header

  if hdr.ExtrasLen > 0 {
    err = binary.Read(r, binary.BigEndian, c.Flags)
    if err != nil {
      return err
    }
  }

  if hdr.KeyLen > 0 {
    c.Key = make([]byte, hdr.KeyLen)
    err = binary.Read(r, binary.BigEndian, c.Key)
    if err != nil {
      return err
    }
  }

  rest := int(hdr.BodyLen) - int(hdr.KeyLen) - int(hdr.ExtrasLen)
  if rest > 0 {
    c.Value = make([]byte, rest)
    err = binary.Read(r, binary.BigEndian, c.Value)
    if err != nil {
      return err
    }
  }
  return nil
}

type SetCmd struct {
  s          *Server
  Header     *RequestHeader
  Flags      uint32
  Expiration uint32
  Key        []byte
  Value      []byte
}

func (c *SetCmd) Decode(r io.Reader) error {
  var err error
  hdr := c.Header

  if hdr.ExtrasLen > 0 {
    err = binary.Read(r, binary.BigEndian, c.Flags)
    if err != nil {
      return err
    }
    err = binary.Read(r, binary.BigEndian, c.Expiration)
    if err != nil {
      return err
    }
  }

  if hdr.KeyLen > 0 {
    c.Key = make([]byte, hdr.KeyLen)
    err = binary.Read(r, binary.BigEndian, c.Key)
    if err != nil {
      return err
    }
  }

  rest := int(hdr.BodyLen) - int(hdr.KeyLen) - int(hdr.ExtrasLen)
  if rest > 0 {
    c.Value = make([]byte, rest)
    err = binary.Read(r, binary.BigEndian, c.Value)
    if err != nil {
      return err
    }
  }
  return nil
}

func (hdr *RequestHeader) decode(r io.Reader) error {
  hdrFields := []interface{}{
    &hdr.Magic, &hdr.Opcode, &hdr.KeyLen, &hdr.ExtrasLen, &hdr.DataType,
    &hdr.Reserved, &hdr.BodyLen, &hdr.Opaque, hdr.CAS,
  }

  for _, field := range hdrFields {
    if err := binary.Read(r, binary.BigEndian, field); err != nil {
      return err
    }
  }
  return nil
}
