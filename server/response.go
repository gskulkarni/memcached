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

// type CmdGetResp struct {
//   Header ResponseHeader
//   Flags  uint32
//   Key    []byte
//   Value  []byte
// }

// func (rsp *CmdGetResp) Write(w io.Writer) error {
//   if err := rsp.Header.encode(w); err != nil {
//     return nil
//   }
//   return writeFields(w, &rsp.Flags)
// }

// type CmdSetResp struct {
//   Header ResponseHeader
//   Extras []byte
//   Key    []byte
//   Value  []byte
// }

// func (rsp *CmdSetResp) Write(w io.Writer) error {
//   if err := rsp.Header.encode(w); err != nil {
//     return nil
//   }
//   return writeFields(w, rsp.Extras, rsp.Key, rsp.Value)
// }

type Response struct {
  Header ResponseHeader
  Flags  uint32
  Key    []byte
  Value  []byte
}

func (hdr *ResponseHeader) encode(w io.Writer) error {
  hdrFields := []interface{}{
    &hdr.Magic, &hdr.Opcode, &hdr.KeyLen, &hdr.ExtrasLen, &hdr.DataType,
    &hdr.Status, &hdr.BodyLen, &hdr.Opaque, &hdr.CAS,
  }
  return writeFields(w, hdrFields...)
}

func (rsp *Response) encode(w io.Writer) error {
  var err error
  hdr := &rsp.Header

  if err = hdr.encode(w); err != nil {
    return err
  }

  fields := []interface{}{}

  if hdr.ExtrasLen > 0 {
    fields = append(fields, &rsp.Flags)
  }

  if len(rsp.Key) > 0 {
    fields = append(fields, rsp.Key)
  }

  if len(rsp.Value) > 0 {
    fields = append(fields, rsp.Value)
  }
  return writeFields(w, fields...)
}

func writeFields(w io.Writer, fields ...interface{}) error {
  for _, f := range fields {
    if err := binary.Write(w, binary.BigEndian, f); err != nil {
      return err
    }
  }
  return nil
}

func (rsp *Response) fillHeader(reqHdr *RequestHeader) error {
  hdr := &rsp.Header
  hdr.Magic = MagicCodeResponse
  hdr.Opaque = reqHdr.Opaque
  hdr.Opcode = reqHdr.Opcode
  hdr.KeyLen = uint16(len(rsp.Key))
  if hdr.Opcode == CmdGet {
    hdr.ExtrasLen = 4
  }
  hdr.BodyLen = uint32(hdr.ExtrasLen) +
    uint32(len(rsp.Key)) + uint32(len(rsp.Value))

  return nil
}
