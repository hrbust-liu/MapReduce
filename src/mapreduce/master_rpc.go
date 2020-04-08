package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

func (mr *Master) Shutdown(_, _*struct{}) error {
	close(mr.shutdown)
	mr.l.Close()
	return nil
}
// 开启RPC等待shutdown
func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.address)
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Fatal("RegstrationServer", mr.address, " error: ", e)
	}
	mr.l = l

	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				break
			}
		}
	}()
}
// 停止RPC
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
}
