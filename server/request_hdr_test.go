package server

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

type hdrTest struct {
	bytes  []byte
	expHdr *RequestHeader
}

func TestRequestHeaderBuild(t *testing.T) {
	tests := []hdrTest{
		{
			bytes: []byte{
				0x80, 0x00, 0x00, 0x05,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x05,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				// 'H', 'e', 'l', 'l',
				// 'o',
			},
			expHdr: &RequestHeader{
				Magic:   0x80,
				KeyLen:  5,
				BodyLen: 5,
			},
		},
		// TODO(sunil): Add more test cases
	}

	for _, tt := range tests {
		hdr := &RequestHeader{}
		err := hdr.read(bytes.NewBuffer(tt.bytes))
		assert.NoError(t, err)
		assert.Equal(t, hdr, tt.expHdr)
	}
}
