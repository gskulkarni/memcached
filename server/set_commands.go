package server

import (
  "encoding/binary"
  "github.com/golang/glog"
  "io"
)

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
    err = binary.Read(r, binary.BigEndian, &c.Flags)
    if err != nil {
      return err
    }
    err = binary.Read(r, binary.BigEndian, &c.Expiration)
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

  key := string(cmd.Key)
  value := cmd.Value
  glog.Infof("got set command for key: %s value: %v header:%+v",
    key, value, cmd.Header)
  err := s.ds.Set(key, value, cmd.Flags, cmd.Expiration, cmd.Header.CAS)
  if err != nil {
    glog.Errorf("error setting key value :: %v", err)
    return nil, err
  }
  rsp.fillHeader(cmd.Header)
  return rsp, nil
}