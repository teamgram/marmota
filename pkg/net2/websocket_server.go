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
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/teamgram/marmota/pkg/net2/websocket"
	log "github.com/zeromicro/go-zero/core/logx"
)

type WebsocketConfig struct {
	Addrs        []string
	ServerName   string
	ProtoName    string
	SendBuf      int
	ReceiveBuf   int
	Keepalive    bool
	SendChanSize int
}

type WebsocketServer struct {
	c        *WebsocketConfig
	accepts  int
	connMgr  *ConnectionManager
	lsnList  []*net.TCPListener
	callback TcpConnectionCallback
	running  bool
}

func NewWebsocketServer(c *WebsocketConfig, accepts int, cb TcpConnectionCallback) (s *WebsocketServer, err error) {
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

	s = &WebsocketServer{
		c:        c,
		accepts:  accepts,
		connMgr:  NewConnectionManager(),
		lsnList:  lsnList,
		callback: cb,
	}

	return
}

func (s *WebsocketServer) Serve() {
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

					//conn2 := NewBufferedConn(conn)
					//codec, err := NewCodecByName(s.c.ProtoName, conn2)
					//if err != nil {
					//	log.Errorf("newCodecByName(%s) error(%v)", s.c.ProtoName, err.Error())
					//	conn.Close()
					//	return
					//}
					//
					////tcpConn := NewTcpConnection2(s.c.ServerName, conn2, s.c.SendChanSize, codec, s)
					go s.establishWebsocketConnection(conn)
				}
			}(lsn)
		}
	}
}

func (s *WebsocketServer) Stop() {
	if s.running {
		for _, lsn := range s.lsnList {
			lsn.Close()
		}
		s.connMgr.Dispose()
		s.running = true
	}
}

func (s *WebsocketServer) Pause() {
}

func (s *WebsocketServer) OnConnectionClosed(conn Connection) {
	s.onConnectionClosed(conn.(*TcpConnection))
}

func (s *WebsocketServer) establishWebsocketConnection(conn net.Conn) {
	var (
		// conn2         = NewBufferedConnSize(conn, 66560)
		req           *websocket.Request
		websocketConn *websocket.Conn
		err           error
	)

	rr := bufio.NewReader(conn)
	if req, err = websocket.ReadRequest(rr); err != nil || req.RequestURI != "/apiws" {
		conn.Close()
		if err != io.EOF {
			log.Errorf("websocket.ReadRequest(rr) error(%v)", err)
		}
		return
	} else {
		wr := bufio.NewWriter(conn)
		websocketConn, err = websocket.Upgrade(conn, rr, wr, req)
		if err != nil {
			log.Errorf("websocket.Upgrade(rr) error(%v)", err)
			conn.Close()
			return
		}
	}

	codec, err := NewCodecByName(s.c.ProtoName, websocketConn)
	tcpConn := NewTcpConnection2(s.c.ServerName, websocketConn, s.c.SendChanSize, codec, true, s)

	// log.Info("establishTcpConnection...")
	defer func() {
		if err := recover(); err != nil {
			log.Error("tcp_server handle panic: %v\n%s", err, debug.Stack())
			tcpConn.Close()
		}
	}()

	s.onNewConnection(tcpConn)

	for {
		tcpConn.conn.SetReadDeadline(time.Now().Add(time.Minute * 6))
		msg, err := tcpConn.Receive()
		if err != nil {
			log.Error("conn: %s recv error: %v", tcpConn, err)
			return
		}

		if msg == nil {
			// 是否需要关闭？
			log.Error("recv a nil msg by conn: %s", tcpConn)
			continue
		}

		if s.callback != nil {
			// log.Info("onConnectionDataArrived - conn: %s", conn)
			if err := s.callback.OnConnectionDataArrived(tcpConn, msg); err != nil {
				// TODO: 是否需要关闭?
			}
		}
	}
}

func (s *WebsocketServer) onNewConnection(conn *TcpConnection) {
	// log.Info("onNewConnection - conn: %s", conn)
	if s.connMgr != nil {
		s.connMgr.putConnection(conn)
	}

	if s.callback != nil {
		s.callback.OnNewConnection(conn)
	}
}

func (s *WebsocketServer) onConnectionClosed(conn *TcpConnection) {
	// log.Info("onConnectionClosed - conn: %s", conn)
	if s.connMgr != nil {
		s.connMgr.delConnection(conn)
	}

	if s.callback != nil {
		s.callback.OnConnectionClosed(conn)
	}
}

func (s *WebsocketServer) SendByConnID(connID uint64, msg interface{}) error {
	conn := s.connMgr.GetConnection(connID)
	if conn == nil {
		return fmt.Errorf("can not get connection(%d)", connID)
	}
	return conn.Send(msg)
}

func (s *WebsocketServer) GetConnection(connID uint64) *TcpConnection {
	conn := s.connMgr.GetConnection(connID)
	if conn != nil {
		return conn.(*TcpConnection)
	}
	return nil
}

//func (s *TcpServer2) acquire() { s.sem <- struct{}{} }
//func (s *TcpServer2) release() { <-s.sem }
