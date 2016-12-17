package main

import (
  "flag"
  "github.com/golang/glog"
  "net"
  "os"
)

var (
  addr = flag.String("addr", ":9090", "address to listen on. default all interface with 9090 port")
)

// server represents a Memcached server.
type server struct {
  addr string
  ln   net.Listener
  ds   *dataStore
}

var ds = NewDataStore()

func main() {
  flag.Parse()

  glog.Infof("preparing to start memcached server")

  ln, err := net.Listen("tcp", *addr)
  if err != nil {
    glog.Errorf("error listening on addr: %s :: %v", *addr, err)
    os.Exit(1)
  }

  done := make(chan struct{})

  for {
    conn, err := ln.Accept()
    if err != nil {
      glog.Errorf("error in accept :: %v", err)
      // TODO(sunil): examine error to check if it parmanent or temporary
      // in case of permanent error, we should exit.
      continue
    }
    go handleConnection(conn)
    // select {
    // case <-done:
    //   glog.Infof("server exiting..")
    //   break
    //   default:
    //   br
    // }
  }
  // TODO(sunil): listen for signals (CTRL-C or kill) for graceful shutdown

  // wait indefinitely here
  <-done
}

// TODO(sunil): this should take some context from the caller, so that caller
// can signal for termination. That will be used in the event of the server
// going down.
func handleConnection(conn net.Conn) {
  glog.Infof("new client: %v connected", conn.RemoteAddr())

  // start the request handler loop for this connection
  for {
    // read the request
    req := &Request{}
    err := req.decode(conn)
    if err != nil {
      // for now, we will terminate the client connection instead of
      // recovering the error
      conn.Close()
      return
    }
    rsp, err := handleRequest(req)
    if err != nil {
      // for now, we will have simple error handling
      // indicate error to the client and continue
      continue
    }
    glog.Infof("got response :: %v", rsp)
    // write response to the client
    err = rsp.encode(conn)
    if err != nil {
      conn.Close()
      return
    }
  }
}

func handleRequest(req *Request) (rsp *Response, err error) {
  // validate the request
  switch req.Header.Opcode {
  case CmdGet:
    rsp, err = handleGetCommand(req)
  case CmdSet:
    rsp, err = handleSetCommand(req)
  default:
    // respond with not implemented error or something
  }
  return
}

func validateGetCommand(req *Request) bool {
  hdr := &req.Header

  isValid := true

  if hdr.ExtrasLen > 0 {
    isValid = false
  }

  if hdr.KeyLen <= 0 {
    isValid = false
  }

  if len(req.Key) == 0 {
    isValid = false
  }

  if len(req.Value) > 0 {
    isValid = false
  }

  return isValid
}

func handleGetCommand(req *Request) (*Response, error) {
  rsp := &Response{}
  if !validateGetCommand(req) {
    // not a valid command
  }
  key := string(req.Key)
  v, found := ds.Get(key)
  if !found {
    glog.V(2).Infof("key: %s not found", key)
  }
  glog.Infof("got get command:%+v for key: %s and found value: %v", req, key, v)
  rsp.Value = v
  rsp.Extras = make([]byte, 4)
  rsp.fillHeader(req)
  return rsp, nil
}

func validateSetCommand(req *Request) bool {
  hdr := &req.Header

  isValid := true

  if hdr.ExtrasLen == 0 {
    isValid = false
  }

  if hdr.KeyLen <= 0 {
    isValid = false
  }

  if len(req.Key) == 0 {
    isValid = false
  }

  if len(req.Value) == 0 {
    isValid = false
  }

  return isValid
}

func (rsp *Response) fillHeader(req *Request) error {
  hdr := &rsp.Header
  hdr.Magic = 0x81
  hdr.Opcode = req.Header.Opcode
  hdr.KeyLen = uint16(len(rsp.Key))
  hdr.ExtrasLen = uint8(len(rsp.Extras))
  hdr.BodyLen = uint32(len(rsp.Extras)) +
    uint32(len(rsp.Key)) + uint32(len(rsp.Value))

  return nil
}

func handleSetCommand(req *Request) (*Response, error) {
  rsp := &Response{}

  if !validateSetCommand(req) {
    // not a valid command
  }
  key := string(req.Key)
  value := req.Value
  glog.Infof("got set command for key: %s value: %v", key, value)
  err := ds.Set(key, value)
  if err != nil {
    glog.Errorf("error setting key value :: %v", err)
    return nil, err
  }
  rsp.fillHeader(req)
  return rsp, nil
}
