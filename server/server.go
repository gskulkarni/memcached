package server

import (
  "fmt"
  "github.com/golang/glog"
  "io"
  "net"
)

// default address for Memcache server.
const DefaultAddr = ":9090"

// Server represents a Memcache server.
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

// TODO(sunil): this should take some context from the caller, so that caller
// can signal for termination. That will be used in the event of the server
// going down.
func (s *Server) serveConnection(conn net.Conn) {
  glog.Infof("new client: %v connected", conn.RemoteAddr())

  // start the request handler loop for this connection
  for {
    rsp, err := s.handleCommand(conn)
    if err != nil {
      conn.Close()
      return
    }
    // write response to the client
    err = rsp.Write(conn)
    if err != nil {
      conn.Close()
      return
    }
  }
}

// handleCommand serves given command. Error values indicate if the associated
// client connection needs to be terminated. Any type of IO error during the
// command handling leads to connection close.
func (s *Server) handleCommand(rw io.ReadWriter) (CommandRspWriter, error) {

  defer func() {
    if err := recover(); err != nil {
      glog.Errorf("recovered from panic ")
    }
  }()

  hdr := &RequestHeader{}
  // read the request header
  err := hdr.read(rw)
  if err != nil {
    return nil, err
  }

  cmd, err := s.getCommand(hdr)
  if err != nil {
    return nil, err
  }

  err = cmd.Read(rw)
  if err != nil {
    return nil, err
  }

  isValid, reason := cmd.IsValid()
  if !isValid {
    // create the response, and return
    return nil, fmt.Errorf("invalid request :: %s", reason)
  }

  return cmd.Execute()
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

func (s *Server) Stop() {
  // TODO(sunil): to be implemented
}
