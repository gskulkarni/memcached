package main

import (
  "flag"
  "github.com/droot/memcached/server"
  "github.com/golang/glog"
  "os"
)

var (
  // command line flag for address that server will listen on.
  addr = flag.String("addr", server.DefaultAddr,
    "address to listen on. default all interface with 9090 port")
)

func main() {
  flag.Parse()

  s := server.New(*addr)

  if err := s.ListenAndServe(); err != nil {
    glog.Errorf("error listening on addr: %s :: %v", *addr, err)
    os.Exit(1)
  }

  glog.Infof("memcached server started on %s", *addr)

  // TODO(sunil): listen for signals (CTRL-C or kill) for graceful shutdown
}
