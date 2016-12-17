package main

import (
  "encoding/binary"
  "io"
)

// this file implements Memcached Request object and related functionality
/*
Request header:

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Magic         | Opcode        | Key length                    |
       +---------------+---------------+---------------+---------------+
      4| Extras length | Data type     | Reserved                      |
       +---------------+---------------+---------------+---------------+
      8| Total body length                                             |
       +---------------+---------------+---------------+---------------+
     12| Opaque                                                        |
       +---------------+---------------+---------------+---------------+
     16| CAS                                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
       Total 24 bytes
*/

type RequestHeader struct {
  Magic     uint8
  Opcode    uint8
  KeyLen    uint16
  ExtrasLen uint8
  DataType  uint8
  Reserved  uint16
  BodyLen   uint32
  Opaque    uint32
  // because binary.Read does not support fix size byte array
  // Ideally it should be [8]byte, but this is enforced in the
  // decode method.
  CAS []byte
}

type Request struct {
  Header RequestHeader
  Extras []byte
  Key    []byte
  Value  []byte
}

const (
  MagicCodeRequest  = 0x80
  MagicCodeResponse = 0x81
)

const (
  CmdGet = 0x00
  CmdSet = 0x01
  // and rest of the codes....

  CmdGetQ = 0x09
  CmdSetQ = 0x11
)

func (hdr *RequestHeader) decode(r io.Reader) error {
  hdr.CAS = make([]byte, 8)
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

// decode reads request content from given reader object and
// returns well formed request object.
func (req *Request) decode(r io.Reader) error {
  var err error
  hdr := &req.Header

  if err = hdr.decode(r); err != nil {
    return err
  }

  if hdr.ExtrasLen > 0 {
    req.Extras = make([]byte, hdr.ExtrasLen)
    err = binary.Read(r, binary.BigEndian, req.Extras)
    if err != nil {
      return err
    }
  }

  if hdr.KeyLen > 0 {
    req.Key = make([]byte, hdr.KeyLen)
    err = binary.Read(r, binary.BigEndian, req.Key)
    if err != nil {
      return err
    }
  }

  rest := int(hdr.BodyLen) - int(hdr.KeyLen) - int(hdr.ExtrasLen)
  if rest > 0 {
    req.Value = make([]byte, rest)
    err = binary.Read(r, binary.BigEndian, req.Value)
    if err != nil {
      return err
    }
  }
  return nil
}
