// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: teamgramio (teamgram.io@gmail.com)
//

package net2

import (
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	log "github.com/zeromicro/go-zero/core/logx"
)

const maxConcurrentConnection = 100000

type TcpConnectionCallback interface {
	OnNewConnection(conn *TcpConnection)
	OnConnectionDataArrived(c *TcpConnection, msg interface{}) error
	OnConnectionClosed(c *TcpConnection)
}

type TcpServer struct {
	connectionManager *ConnectionManager
	listener          net.Listener
	serverName        string
	protoName         string
	sendChanSize      int
	callback          TcpConnectionCallback
	running           bool
	sem               chan struct{}
	releaseOnce       sync.Once
}

type TcpServerArgs struct {
	Listener                net.Listener
	ServerName              string
	ProtoName               string
	SendChanSize            int
	ConnectionCallback      TcpConnectionCallback
	MaxConcurrentConnection int
}

func NewTcpServer(args TcpServerArgs) *TcpServer {
	if args.MaxConcurrentConnection < 1 {
		args.MaxConcurrentConnection = maxConcurrentConnection
	}
	return &TcpServer{
		connectionManager: NewConnectionManager(),
		listener:          args.Listener,
		serverName:        args.ServerName,
		protoName:         args.ProtoName,
		sendChanSize:      args.SendChanSize,
		callback:          args.ConnectionCallback,
		running:           false,
		sem:               make(chan struct{}, args.MaxConcurrentConnection),
	}
}

func (s *TcpServer) Serve() {
	if s.running {
		return
	}
	s.running = true
	s.acquire()

	for {
		conn, err := Accept(s.listener)
		if err != nil {
			log.Error(err.Error())
			return
		}

		codec, err := NewCodecByName(s.protoName, conn)
		if err != nil {
			log.Error(err.Error())
			conn.Close()
			return
		}

		tcpConn := NewTcpConnection(s.serverName, conn, s.sendChanSize, codec, s)
		go s.establishTcpConnection(tcpConn)
	}

	s.running = false
}

// TODO(@benqi): 讨巧的办法
func (s *TcpServer) Serve2() {
	if s.running {
		return
	}
	s.running = true
	s.acquire()

	for {
		conn, err := Accept(s.listener)
		if err != nil {
			log.Error(err.Error())
			return
		}

		conn2 := NewBufferedConn(conn)
		codec, err := NewCodecByName(s.protoName, conn2)
		if err != nil {
			log.Error(err.Error())
			conn.Close()
			return
		}

		tcpConn := NewTcpConnection(s.serverName, conn2, s.sendChanSize, codec, s)
		go s.establishTcpConnection(tcpConn)
	}

	s.running = false
}

func Accept(listener net.Listener) (net.Conn, error) {
	var tempDelay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				return nil, io.EOF
			}
			return nil, err
		}
		return conn, nil
	}
}

func (s *TcpServer) Stop() {
	if s.running {
		s.listener.Close()
		s.connectionManager.Dispose()
		s.releaseOnce.Do(s.release)
	}
}

func (s *TcpServer) Pause() {
}

func (s *TcpServer) OnConnectionClosed(conn Connection) {
	s.onConnectionClosed(conn.(*TcpConnection))
}

func (s *TcpServer) establishTcpConnection(conn *TcpConnection) {
	log.Info("establishTcpConnection...")
	defer func() {
		//
		if err := recover(); err != nil {
			log.Error("tcp_server handle panic: %v\n%s", err, debug.Stack())
			conn.Close()
		}
	}()

	s.onNewConnection(conn)

	for {
		conn.conn.SetReadDeadline(time.Now().Add(time.Minute * 6))
		msg, err := conn.Receive()
		if err != nil {
			log.Error("conn {%v} recv error: %v", conn, err)
			return
		}

		if msg == nil {
			// 是否需要关闭？
			log.Error("recv a nil msg: %v", conn)
			continue
		}

		if s.callback != nil {
			if err := s.callback.OnConnectionDataArrived(conn, msg); err != nil {
				// TODO: 是否需要关闭?
			}
		}
	}
}

func (s *TcpServer) onNewConnection(conn *TcpConnection) {
	if s.connectionManager != nil {
		s.connectionManager.putConnection(conn)
	}

	if s.callback != nil {
		s.callback.OnNewConnection(conn)
	}
}

func (s *TcpServer) onConnectionClosed(conn *TcpConnection) {
	if s.connectionManager != nil {
		s.connectionManager.delConnection(conn)
	}

	if s.callback != nil {
		s.callback.OnConnectionClosed(conn)
	}
}

func (s *TcpServer) SendByConnID(connID uint64, msg interface{}) error {
	conn := s.connectionManager.GetConnection(connID)
	if conn == nil {
		return fmt.Errorf("can not get connection(%d)", connID)
	}
	return conn.Send(msg)
}

func (s *TcpServer) GetConnection(connID uint64) *TcpConnection {
	conn := s.connectionManager.GetConnection(connID)
	if conn != nil {
		return conn.(*TcpConnection)
	}
	return nil
}

func (s *TcpServer) acquire() { s.sem <- struct{}{} }
func (s *TcpServer) release() { <-s.sem }
