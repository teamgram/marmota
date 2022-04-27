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
	"database/sql"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/breaker"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

//const (
//	_family          = "sql_client"
//	_slowLogDuration = time.Millisecond * 250
//)

var (
	//	// ErrStmtNil prepared stmt error
	//	ErrStmtNil = errors.New("sql: prepare failed and stmt nil")
	// ErrNoMaster is returned by Master when call master multiple times.
	ErrNoMaster = errors.New("sql: no master instance")

//	// ErrNoRows is returned by Scan when QueryRow doesn't return a row.
//	// In such a case, QueryRow returns a placeholder *Row value that defers
//	// this error until a Scan.
//	ErrNoRows = sql.ErrNoRows
//	// ErrTxDone transaction done.
//	ErrTxDone = sql.ErrTxDone
)

// DB database.
type DB struct {
	write  *conn
	read   []*conn
	idx    uint64
	master *DB
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a database
// name and connection information.
func Open(c *Config) (*DB, error) {
	db := new(DB)
	w := newConn(sqlx.NewMysql(c.DSN))
	rs := make([]*conn, 0, len(c.ReadDSN))
	for _, rd := range c.ReadDSN {
		r := newConn(sqlx.NewMysql(rd))
		rs = append(rs, r)
	}
	db.write = w
	db.read = rs
	db.master = &DB{write: db.write}
	return db, nil
}

// Master return *DB instance direct use master conn
// use this *DB instance only when you have some reason need to get result without any delay.
func (db *DB) Master() *DB {
	if db.master == nil {
		panic(ErrNoMaster)
	}
	return db.master
}

func (db *DB) readIndex() int {
	if len(db.read) == 0 {
		return 0
	}
	v := atomic.AddUint64(&db.idx, 1)
	return int(v % uint64(len(db.read)))
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) Exec(c context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	return db.write.Exec(c, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExec(c context.Context, query string, arg interface{}) (res sql.Result, err error) {
	return db.write.NamedExec(c, query, arg)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepare(c context.Context, query string) (sqlx.StmtSession, error) {
	return db.write.Prepare(c, query)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (db *DB) QueryRow(c context.Context, v interface{}, query string, args ...interface{}) (err error) {
	idx := db.readIndex()
	for i := range db.read {
		err = db.read[(idx+i)%len(db.read)].QueryRow(c, v, query, args...)
		if err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write.QueryRow(c, v, query, args...)
}

// NamedQueryRow using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQueryRow(c context.Context, v interface{}, query string, arg interface{}) error {
	idx := db.readIndex()
	for i := range db.read {
		err := db.read[(idx+i)%len(db.read)].NamedQueryRow(c, v, query, arg)
		if err != breaker.ErrServiceUnavailable {
			return err
		}
	}
	return db.write.NamedQueryRow(c, v, query, arg)
}

func (db *DB) QueryRowPartial(c context.Context, v interface{}, query string, args ...interface{}) error {
	idx := db.readIndex()
	for i := range db.read {
		err := db.read[(idx+i)%len(db.read)].QueryRowPartial(c, v, query, args...)
		if err != breaker.ErrServiceUnavailable {
			return err
		}
	}
	return db.write.QueryRowPartial(c, v, query, args...)
}

func (db *DB) NamedQueryRowPartial(ctx context.Context, v interface{}, query string, arg interface{}) error {
	idx := db.readIndex()
	for i := range db.read {
		err := db.read[(idx+i)%len(db.read)].NamedQueryRowPartial(ctx, v, query, arg)
		if err != breaker.ErrServiceUnavailable {
			return err
		}
	}
	return db.write.NamedQueryRowPartial(ctx, v, query, arg)
}

// QueryRows executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (db *DB) QueryRows(c context.Context, v interface{}, query string, args ...interface{}) (err error) {
	idx := db.readIndex()
	for i := range db.read {
		err = db.read[(idx+i)%len(db.read)].QueryRows(c, v, query, args...)
		if err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write.QueryRows(c, v, query, args...)
}

func (db *DB) NamedQueryRows(c context.Context, v interface{}, query string, arg interface{}) error {
	idx := db.readIndex()
	for i := range db.read {
		err := db.read[(idx+i)%len(db.read)].NamedQueryRows(c, v, query, arg)
		if err != breaker.ErrServiceUnavailable {
			return err
		}
	}
	return db.write.NamedQueryRows(c, v, query, arg)
}

func (db *DB) QueryRowsPartial(c context.Context, v interface{}, query string, args ...interface{}) (err error) {
	idx := db.readIndex()
	for i := range db.read {
		err = db.read[(idx+i)%len(db.read)].QueryRowsPartial(c, v, query, args...)
		if err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write.QueryRowsPartial(c, v, query, args...)
}

func (db *DB) NamedQueryRowsPartial(c context.Context, v interface{}, query string, arg interface{}) (err error) {
	idx := db.readIndex()
	for i := range db.read {
		err = db.read[(idx+i)%len(db.read)].NamedQueryRowsPartial(c, v, query, arg)
		if err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write.NamedQueryRowsPartial(c, v, query, arg)
}

func (db *DB) Transact(ctx context.Context, fn func(*Tx) error) error {
	return db.write.Transact(ctx, fn)
}
