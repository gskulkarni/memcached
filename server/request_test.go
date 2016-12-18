package server

import (
  "bytes"
  "github.com/stretchr/testify/assert"
  "testing"
)

type reqTest struct {
  bytes  []byte
  expReq *Request
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
      expReq: &Request{
        Header: RequestHeader{
          Magic:   0x80,
          KeyLen:  5,
          BodyLen: 5,
          CAS:     make([]byte, 8),
        },
        Key: []byte("Hello"),
      },
    },
    // TODO(sunil): Add more test cases
  }

  for _, tt := range tests {
    req := &Request{}
    err := req.decode(bytes.NewBuffer(tt.bytes))
    assert.NoError(t, err)
    assert.Equal(t, req, tt.expReq)
  }
}
