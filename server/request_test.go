package server

import (
  "bytes"
  "github.com/stretchr/testify/assert"
  "testing"
)

type reqTest struct {
  bytes  []byte
  expCmd *GetCmd
  err    error
}

func TestRequestDecode(t *testing.T) {
  tests := []reqTest{
    {
      bytes: []byte{
        0x80, 0x00, 0x00, 0x05,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x05,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        'H', 'e', 'l', 'l',
        'o',
      },
      expCmd: &GetCmd{
        Header: &RequestHeader{
          Magic:   0x80,
          KeyLen:  5,
          BodyLen: 5,
        },
        Key: []byte("Hello"),
      },
    },
    // TODO(sunil): Add more test cases
  }

  for _, tt := range tests {
    r := bytes.NewBuffer(tt.bytes)
    hdr := &RequestHeader{}
    err := hdr.decode(r)
    // cmd := &GetCmd{}
    // err := cmd.Decode(r)
    assert.NoError(t, err)
    assert.Equal(t, hdr, tt.expCmd.Header)
    // assert.Equal(t, cmd, tt.expCmd)
  }
}
