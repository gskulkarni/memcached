package server

import (
  "encoding/binary"
  "io"
)

// constants for Status code for response.
const (
  StatusNoError       = 0x0000
  StatusKeyError      = 0x0001
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

type Response struct {
  Header ResponseHeader
  Extras []byte
  Key    []byte
  Value  []byte
}

func (hdr *ResponseHeader) encode(w io.Writer) error {
  hdrFields := []interface{}{
    &hdr.Magic, &hdr.Opcode, &hdr.KeyLen, &hdr.ExtrasLen, &hdr.DataType,
    &hdr.Status, &hdr.BodyLen, &hdr.Opaque, hdr.CAS,
  }

  for _, field := range hdrFields {
    if err := binary.Write(w, binary.BigEndian, field); err != nil {
      return err
    }
  }
  return nil
}

func (rsp *Response) encode(w io.Writer) error {
  var err error
  hdr := &rsp.Header

  // TODO (sunil): perform len validation of various fields

  if err = hdr.encode(w); err != nil {
    return err
  }

  if len(rsp.Extras) > 0 {
    err = binary.Write(w, binary.BigEndian, rsp.Extras)
    if err != nil {
      return err
    }
  }

  if len(rsp.Key) > 0 {
    err = binary.Write(w, binary.BigEndian, rsp.Key)
    if err != nil {
      return err
    }
  }

  //   rest := int(hdr.BodyLen) - int(hdr.KeyLen) - int(hdr.ExtrasLen)
  if len(rsp.Value) > 0 {
    err = binary.Write(w, binary.BigEndian, rsp.Value)
    if err != nil {
      return err
    }
  }
  return nil
}

func (rsp *Response) fillHeader(reqHdr *RequestHeader) error {
  hdr := &rsp.Header
  hdr.Magic = 0x81
  hdr.Opaque = reqHdr.Opaque
  hdr.Opcode = reqHdr.Opcode
  hdr.KeyLen = uint16(len(rsp.Key))
  hdr.ExtrasLen = uint8(len(rsp.Extras))
  hdr.BodyLen = uint32(len(rsp.Extras)) +
    uint32(len(rsp.Key)) + uint32(len(rsp.Value))

  return nil
}
