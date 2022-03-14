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
	"net"
)

type testUdpEchoServer struct {
	server      *UdpServer
	serverName  string
	workLoadCnt int
}

func NewUdpTestServer(listener net.Listener, serverName, protoName string, chanSize int, maxConn int) *testUdpEchoServer {
	s := &testUdpEchoServer{}
	s.server = NewUdpServer(
		UdpServerArgs{
			ServerName:   serverName,
			ProtoName:    protoName,
			SendChanSize: chanSize,
		})
	s.serverName = serverName
	s.workLoadCnt = 0
	return s
}

func (s *testUdpEchoServer) Serve() {
	s.server.Serve()
}

func (s *testUdpEchoServer) Stop() {
	s.server.Stop()
}
