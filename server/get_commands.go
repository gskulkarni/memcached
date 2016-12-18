package server

import (
  "encoding/binary"
  "github.com/golang/glog"
  "io"
)

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
    err = binary.Read(r, binary.BigEndian, &c.Flags)
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
  key := string(cmd.Key)
  item, found := s.ds.Get(key)
  if !found {
    glog.V(2).Infof("key: %s not found", key)
    rsp.Header.Status = StatusKeyNotFound
    rsp.Value = []byte("Item not found")
  } else {
    rsp.Value = item.value
    rsp.Flags = item.flags
    rsp.Header.CAS = item.cas
  }
  // glog.Infof("got get command:%+v for key: %s and found value: %v", req, key, v)
  rsp.fillHeader(cmd.Header)
  return rsp, nil
}
