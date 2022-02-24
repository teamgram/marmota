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

package sqlx

import (
	"context"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/teamgram/marmota/pkg/time"
)

// Config mysql config.
type Config struct {
	Addr         string        // for trace
	DSN          string        // write data source name.
	ReadDSN      []string      `json:",optional"` // read data source name.
	Active       int           `json:",optional"` // pool
	Idle         int           `json:",optional"` // pool
	IdleTimeout  string        `json:",optional"` // connect max life time.
	QueryTimeout string        `json:",optional"` // query sql timeout
	ExecTimeout  string        `json:",optional"` // execute sql timeout
	TranTimeout  string        `json:",optional"` // transaction sql timeout
	idleTimeout  time.Duration `json:",optional"` // connect max life time.
	queryTimeout time.Duration `json:",optional"` // query sql timeout
	execTimeout  time.Duration `json:",optional"` // execute sql timeout
	tranTimeout  time.Duration `json:",optional"` // transaction sql timeout
	// Breaker      *breaker.Config // breaker
}

func (c *Config) resetTimeout() {
	c.idleTimeout.UnmarshalText([]byte(c.IdleTimeout))
	c.queryTimeout.UnmarshalText([]byte(c.QueryTimeout))
	c.execTimeout.UnmarshalText([]byte(c.ExecTimeout))
	c.tranTimeout.UnmarshalText([]byte(c.TranTimeout))
}

// NewMySQL new db and retry connection when has error.
func NewMySQL(c *Config) (db *DB) {
	// hack
	c.resetTimeout()

	if c.queryTimeout == 0 || c.execTimeout == 0 || c.tranTimeout == 0 {
		panic("mysql must be set query/execute/transction timeout")
	}
	db, err := Open(c)
	if err != nil {
		logx.Error("open mysql error(%v)", err)
		panic(err)
	}
	return
}

// TxWrapper TxWrapper
func TxWrapper(ctx context.Context, db *DB, txF func(*Tx, *StoreResult)) *StoreResult {
	result := &StoreResult{}

	tx, err := db.Begin(ctx)
	if err != nil {
		result.Err = err
		return result
	}

	defer func() {
		if result.Err != nil {
			tx.Rollback()
		} else {
			result.Err = tx.Commit()
		}
	}()

	txF(tx, result)
	return result
}

// IsDuplicate
// Check if MySQL error is a Error Code: 1062. Duplicate entry ... for key ...
func IsDuplicate(err error) bool {
	if err == nil {
		return false
	}

	myerr, ok := err.(*mysql.MySQLError)
	return ok && myerr.Number == 1062
}

func IsMissingDb(err error) bool {
	if err == nil {
		return false
	}

	myerr, ok := err.(*mysql.MySQLError)
	return ok && myerr.Number == 1049
}
