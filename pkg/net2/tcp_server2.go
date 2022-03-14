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
	"time"

	log "github.com/zeromicro/go-zero/core/logx"
)

type TcpServerConfig struct {
	Addrs        []string
	ServerName   string
	ProtoName    string
	SendBuf      int
	ReceiveBuf   int
	Keepalive    bool
	SendChanSize int
}

type TcpServer2 struct {
	c        *TcpServerConfig
	accepts  int
	connMgr  *ConnectionManager
	lsnList  []*net.TCPListener
	callback TcpConnectionCallback
	running  bool
}

func NewTcpServer2(c *TcpServerConfig, accepts int, cb TcpConnectionCallback) (s *TcpServer2, err error) {
	var (
		bind    string
		lsnList []*net.TCPListener
		addr    *net.TCPAddr
	)

	for _, bind = range c.Addrs {
		if addr, err = net.ResolveTCPAddr("tcp", bind); err != nil {
			log.Error(`net.ResolveTCPAddr("tcp", "%s") error(%v)`, bind, err)
			return
		}
		var listener *net.TCPListener
		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			log.Error(`net.ListenTCP("tcp", "%s") error(%v)`, bind, err)
			return
		}
		lsnList = append(lsnList, listener)
	}

	s = &TcpServer2{
		c:        c,
		accepts:  accepts,
		connMgr:  NewConnectionManager(),
		lsnList:  lsnList,
		callback: cb,
	}

	return
}

func AcceptTCP(listener *net.TCPListener) (*net.TCPConn, error) {
	var tempDelay time.Duration
	for {
		conn, err := listener.AcceptTCP()
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
				log.Errorf("acceptTCP error: %v", err.Error())
				continue
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				log.Errorf("acceptTCP error: %v", err.Error())
				return nil, io.EOF
			}
			return nil, err
		}
		return conn, nil
	}
}

func (s *TcpServer2) Serve() {
	if s.running {
		return
	}
	s.running = true

	for _, lsn := range s.lsnList {
		for i := 0; i < s.accepts; i++ {
			go func(lsn2 *net.TCPListener) {
				var (
					conn *net.TCPConn
					err  error
				)
				for {
					if conn, err = AcceptTCP(lsn2); err != nil {
						// if listener close then return
						log.Error("listener.Accept(\"%s\") error(%v)", lsn2.Addr().String(), err)
						return
					}
					if err = conn.SetKeepAlive(s.c.Keepalive); err != nil {
						log.Error("conn.SetKeepAlive() error(%v)", err)
						return
					}
					if err = conn.SetReadBuffer(s.c.ReceiveBuf); err != nil {
						log.Error("conn.SetReadBuffer() error(%v)", err)
						return
					}
					if err = conn.SetWriteBuffer(s.c.SendBuf); err != nil {
						log.Error("conn.SetWriteBuffer() error(%v)", err)
						return
					}

					conn2 := NewBufferedConn(conn)
					codec, err := NewCodecByName(s.c.ProtoName, conn2)
					if err != nil {
						log.Errorf("newCodecByName(%s) error(%v)", s.c.ProtoName, err.Error())
						conn.Close()
						return
					}

					tcpConn := NewTcpConnection2(s.c.ServerName, conn2, s.c.SendChanSize, codec, false, s)
					go s.establishTcpConnection(tcpConn)
				}
			}(lsn)
		}
	}
}

func (s *TcpServer2) Stop() {
	if s.running {
		for _, lsn := range s.lsnList {
			lsn.Close()
		}
		s.connMgr.Dispose()
		s.running = true
	}
}

func (s *TcpServer2) Pause() {
}

func (s *TcpServer2) OnConnectionClosed(conn Connection) {
	s.onConnectionClosed(conn.(*TcpConnection))
}

func (s *TcpServer2) establishTcpConnection(conn *TcpConnection) {
	// log.Info("establishTcpConnection...")
	defer func() {
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
			log.Error("conn: %s recv error: %v", conn, err)
			return
		}

		if msg == nil {
			// 是否需要关闭？
			log.Error("recv a nil msg by conn: %s", conn)
			continue
		}

		if s.callback != nil {
			// log.Info("onConnectionDataArrived - conn: %s", conn)
			if err := s.callback.OnConnectionDataArrived(conn, msg); err != nil {
				// TODO: 是否需要关闭?
			}
		}
	}
}

func (s *TcpServer2) onNewConnection(conn *TcpConnection) {
	// log.Info("onNewConnection - conn: %s", conn)
	if s.connMgr != nil {
		s.connMgr.putConnection(conn)
	}

	if s.callback != nil {
		s.callback.OnNewConnection(conn)
	}
}

func (s *TcpServer2) onConnectionClosed(conn *TcpConnection) {
	// log.Info("onConnectionClosed - conn: %s", conn)
	if s.connMgr != nil {
		s.connMgr.delConnection(conn)
	}

	if s.callback != nil {
		s.callback.OnConnectionClosed(conn)
	}
}

func (s *TcpServer2) SendByConnID(connID uint64, msg interface{}) error {
	conn := s.connMgr.GetConnection(connID)
	if conn == nil {
		return fmt.Errorf("can not get connection(%d)", connID)
	}
	return conn.Send(msg)
}

func (s *TcpServer2) GetConnection(connID uint64) *TcpConnection {
	conn := s.connMgr.GetConnection(connID)
	if conn != nil {
		return conn.(*TcpConnection)
	}
	return nil
}

//func (s *TcpServer2) acquire() { s.sem <- struct{}{} }
//func (s *TcpServer2) release() { <-s.sem }
