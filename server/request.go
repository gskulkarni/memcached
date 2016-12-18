package server

import (
  "encoding/binary"
  "github.com/golang/glog"
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

func (cmd *GetCmd) IsValid() (bool, string) {
  hdr := cmd.Header

  if hdr.ExtrasLen > 0 {
    return false, "extras fields empty"
  }

  if hdr.KeyLen <= 0 {
    return false, "key len 0"
  }

  if len(cmd.Key) == 0 {
    return false, "missing key"
  }

  if len(cmd.Value) > 0 {
    return false, "non-empty value"
  }

  return true, ""
}

func (cmd *GetCmd) Execute() (*Response, error) {
  s := cmd.s
  rsp := &Response{}
  // if !validateGetCommand(req) {
  //   // not a valid command
  // }
  key := string(cmd.Key)
  v, found := s.ds.Get(key)
  if !found {
    glog.V(2).Infof("key: %s not found", key)
  }
  // glog.Infof("got get command:%+v for key: %s and found value: %v", req, key, v)
  rsp.Value = v
  rsp.Extras = make([]byte, 4)
  rsp.fillHeader(cmd.Header)
  return rsp, nil
}

func (cmd *SetCmd) IsValid() (bool, string) {
  hdr := cmd.Header

  if hdr.ExtrasLen == 0 {
    return false, "missing extra length"
  }

  if hdr.KeyLen <= 0 {
    return false, "0 key length"
  }

  if len(cmd.Key) == 0 {
    return false, "missing key"
  }

  if len(cmd.Value) == 0 {
    return false, "missing value"
  }

  return true, ""
}

func (cmd *SetCmd) Execute() (*Response, error) {
  s := cmd.s
  rsp := &Response{}

  // if !validateSetCommand(req) {
  //   // not a valid command
  // }
  key := string(cmd.Key)
  value := cmd.Value
  glog.Infof("got set command for key: %s value: %v", key, value)
  err := s.ds.Set(key, value)
  if err != nil {
    glog.Errorf("error setting key value :: %v", err)
    return nil, err
  }
  rsp.fillHeader(cmd.Header)
  return rsp, nil
}
