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

func (c *GetCmd) Read(r io.Reader) error {
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

func (cmd *GetCmd) Execute() (CommandRspWriter, error) {
  s := cmd.s
  rsp := &CmdGetResp{}
  key := string(cmd.Key)
  item, found := s.ds.get(key)
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

func (rsp *CmdGetResp) fillHeader(reqHdr *RequestHeader) {
  hdr := &rsp.Header
  hdr.Magic = MagicCodeResponse
  hdr.Opaque = reqHdr.Opaque
  hdr.Opcode = reqHdr.Opcode
  hdr.KeyLen = uint16(len(rsp.Key))
  hdr.ExtrasLen = 4
  hdr.BodyLen = uint32(hdr.ExtrasLen) +
    uint32(len(rsp.Key)) + uint32(len(rsp.Value))
}

type CmdGetResp struct {
  Header ResponseHeader
  Flags  uint32
  Key    []byte
  Value  []byte
}

func (rsp *CmdGetResp) Write(w io.Writer) error {
  if err := rsp.Header.write(w); err != nil {
    return nil
  }
  fields := []interface{}{&rsp.Flags}

  if len(rsp.Key) > 0 {
    fields = append(fields, rsp.Key)
  }

  if len(rsp.Value) > 0 {
    fields = append(fields, rsp.Value)
  }
  return writeFields(w, fields...)
}
