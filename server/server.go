package server

import (
  "fmt"
  "github.com/golang/glog"
  "io"
  "net"
)

const DefaultAddr = ":9090"

// Server represents a Memcached server.
type Server struct {
  addr string
  ln   net.Listener
  ds   *dataStore
  done chan struct{}
}

// New creates an instance of Memcached server.
func New(addr string) *Server {
  if addr == "" {
    addr = DefaultAddr
  }
  return &Server{
    addr: DefaultAddr,
    ds:   newDataStore(),
    done: make(chan struct{}),
  }
}

func (s *Server) ListenAndServe() error {
  ln, err := net.Listen("tcp", s.addr)
  if err != nil {
    return err
  }
  s.ln = ln
  return s.serve()
}

func (s *Server) serve() error {
  for {
    conn, err := s.ln.Accept()
    if err != nil {
      glog.Warningf("error in accept new connection :: %v", err)
      // TODO(sunil): examine error to check if it parmanent or temporary
      // in case of permanent error, we should exit.
      continue
    }
    go s.serveConnection(conn)
  }
}

func (s *Server) Stop() {
  // TODO(sunil): to be implemented
}

// TODO(sunil): this should take some context from the caller, so that caller
// can signal for termination. That will be used in the event of the server
// going down.
func (s *Server) serveConnection(conn net.Conn) {
  glog.Infof("new client: %v connected", conn.RemoteAddr())

  // start the request handler loop for this connection
  for {
    _, err := s.handleCommand(conn)
    if err != nil {
      conn.Close()
      return
    }
  }
}

func (s *Server) getCommand(hdr *RequestHeader) (cmd Command, err error) {
  switch hdr.Opcode {
  case CmdGet:
    cmd = &GetCmd{s: s, Header: hdr}
  case CmdSet:
    cmd = &SetCmd{s: s, Header: hdr}
  default:
    err = fmt.Errorf("invalid command")
  }
  return
}

// handleCommand serves given command. Error values indicate if the associated
// client connection needs to be terminated. Any type of IO error during the
// command handling leads to connection close.
func (s *Server) handleCommand(rw io.ReadWriter) (*Response, error) {
  hdr := &RequestHeader{}
  // read the request header
  err := hdr.decode(rw)
  if err != nil {
    return nil, err
  }

  cmd, err := s.getCommand(hdr)
  if err != nil {
    return nil, err
  }

  err = cmd.Decode(rw)
  if err != nil {
    return nil, err
  }

  isValid, reason := cmd.IsValid()
  if !isValid {
    // create the response, and return
    return nil, fmt.Errorf("invalid request :: %s", reason)
  }

  rsp, err := cmd.Execute()
  if err != nil {
    return nil, err
  }

  // write response to the client
  err = rsp.encode(rw)
  if err != nil {
    return nil, err
  }
  return rsp, err
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

func (rsp *Response) fillHeader(reqHdr *RequestHeader) error {
  hdr := &rsp.Header
  hdr.Magic = 0x81
  hdr.Opcode = reqHdr.Opcode
  hdr.KeyLen = uint16(len(rsp.Key))
  hdr.ExtrasLen = uint8(len(rsp.Extras))
  hdr.BodyLen = uint32(len(rsp.Extras)) +
    uint32(len(rsp.Key)) + uint32(len(rsp.Value))

  return nil
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
