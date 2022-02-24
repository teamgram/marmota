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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/teamgram/marmota/pkg/stores/sqlx/reflectx"

	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/breaker"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	_family          = "sql_client"
	_slowLogDuration = time.Millisecond * 250
)

var (
	// ErrStmtNil prepared stmt error
	ErrStmtNil = errors.New("sql: prepare failed and stmt nil")
	// ErrNoMaster is returned by Master when call master multiple times.
	ErrNoMaster = errors.New("sql: no master instance")
	// ErrNoRows is returned by Scan when QueryRow doesn't return a row.
	// In such a case, QueryRow returns a placeholder *Row value that defers
	// this error until a Scan.
	ErrNoRows = sql.ErrNoRows
	// ErrTxDone transaction done.
	ErrTxDone = sql.ErrTxDone
)

// DB database.
type DB struct {
	write  *conn
	read   []*conn
	idx    uint64
	master *DB
}

// conn database connection
type conn struct {
	*sql.DB
	breaker breaker.Breaker
	conf    *Config
	unsafe  bool
	Mapper  *reflectx.Mapper
}

// Tx transaction.
type Tx struct {
	db *conn
	tx *sql.Tx
	// t      tracespec.Trace
	c      context.Context
	cancel func()
}

// Stmt prepared stmt.
type Stmt struct {
	db    *conn
	tx    bool
	query string
	stmt  atomic.Value
	// t     tracespec.Trace
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a database
// name and connection information.
func Open(c *Config) (*DB, error) {
	db := new(DB)
	d, err := connect(c, c.DSN)
	if err != nil {
		return nil, err
	}
	brk := breaker.GetBreaker(c.Addr)
	w := &conn{DB: d, breaker: brk, conf: c, Mapper: mapper()}
	rs := make([]*conn, 0, len(c.ReadDSN))
	for _, rd := range c.ReadDSN {
		d, err := connect(c, rd)
		if err != nil {
			return nil, err
		}
		brk := breaker.GetBreaker(parseDSNAddr(rd))
		r := &conn{DB: d, breaker: brk, conf: c, Mapper: mapper()}
		rs = append(rs, r)
	}
	db.write = w
	db.read = rs
	db.master = &DB{write: db.write}
	return db, nil
}

func connect(c *Config, dataSourceName string) (*sql.DB, error) {
	d, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	// we need to do this until the issue https://github.com/golang/go/issues/9851 get fixed
	// discussed here https://github.com/go-sql-driver/mysql/issues/257
	// if the discussed SetMaxIdleTimeout methods added, we'll change this behavior
	// 8 means we can't have more than 8 goroutines to concurrently access the same database.
	d.SetMaxOpenConns(c.Active)
	d.SetMaxIdleConns(c.Idle)
	d.SetConnMaxLifetime(time.Duration(c.idleTimeout))
	return d, nil
}

// Begin starts a transaction. The isolation level is dependent on the driver.
func (db *DB) Begin(c context.Context) (tx *Tx, err error) {
	return db.write.begin(c)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (db *DB) Exec(c context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	return db.write.exec(c, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExec(c context.Context, query string, args interface{}) (res sql.Result, err error) {
	return db.write.namedExec(c, query, args)
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepare(query string) (*Stmt, error) {
	return db.write.prepare(query)
}

// Prepared creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the returned
// statement. The caller must call the statement's Close method when the
// statement is no longer needed.
func (db *DB) Prepared(query string) (stmt *Stmt) {
	return db.write.prepared(query)
}

// Query executes a query that returns rows, typically a SELECT. The args are
// for any placeholder parameters in the query.
func (db *DB) Query(c context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	idx := db.readIndex()
	for i := range db.read {
		if rows, err = db.read[(idx+i)%len(db.read)].query(c, query, args...); err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write.query(c, query, args...)
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQuery(c context.Context, query string, arg interface{}) (rows *Rows, err error) {
	idx := db.readIndex()
	for i := range db.read {
		if rows, err = db.read[(idx+i)%len(db.read)].namedQuery(c, query, arg); err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write.namedQuery(c, query, arg)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (db *DB) QueryRow(c context.Context, query string, args ...interface{}) *Row {
	idx := db.readIndex()
	for i := range db.read {
		if row := db.read[(idx+i)%len(db.read)].queryRow(c, query, args...); row.err != breaker.ErrServiceUnavailable {
			return row
		}
	}
	return db.write.queryRow(c, query, args...)
}

// Select using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Select(c context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	idx := db.readIndex()
	for i := range db.read {
		if err = db.read[(idx+i)%len(db.read)]._select(c, dest, query, args...); err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write._select(c, dest, query, args...)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) Get(c context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	idx := db.readIndex()
	for i := range db.read {
		if err = db.read[(idx+i)%len(db.read)]._get(c, dest, query, args...); err != breaker.ErrServiceUnavailable {
			return
		}
	}
	return db.write._get(c, dest, query, args...)
}

func (db *DB) readIndex() int {
	if len(db.read) == 0 {
		return 0
	}
	v := atomic.AddUint64(&db.idx, 1)
	return int(v % uint64(len(db.read)))
}

// Close closes the write and read database, releasing any open resources.
func (db *DB) Close() (err error) {
	if e := db.write.Close(); e != nil {
		err = errors.WithStack(e)
	}
	for _, rd := range db.read {
		if e := rd.Close(); e != nil {
			err = errors.WithStack(e)
		}
	}
	return
}

// Ping verifies a connection to the database is still alive, establishing a
// connection if necessary.
func (db *DB) Ping(c context.Context) (err error) {
	if err = db.write.ping(c); err != nil {
		return
	}
	for _, rd := range db.read {
		if err = rd.ping(c); err != nil {
			return
		}
	}
	return
}

// Master return *DB instance direct use master conn
// use this *DB instance only when you have some reason need to get result without any delay.
func (db *DB) Master() *DB {
	if db.master == nil {
		panic(ErrNoMaster)
	}
	return db.master
}

func (db *conn) onBreaker(err *error) {
	if err != nil && *err != nil && *err != sql.ErrNoRows && *err != sql.ErrTxDone {
		// db.breaker.MarkFailed()
		if p, err2 := db.breaker.Allow(); err2 == nil {
			p.Reject((*err).Error())
		}
	} else {
		// db.breaker.MarkSuccess()
		if p, err2 := db.breaker.Allow(); err2 == nil {
			p.Accept()
		}
	}
}

func (db *conn) begin(c context.Context) (tx *Tx, err error) {
	now := time.Now()
	defer slowLog("Begin", now)

	//if t, ok := trace.FromContext(c); ok {
	//if ok {
	//	t = t.Fork(_family, "begin")
	//	t.SetTag(trace.String(trace.TagAddress, db.conf.Addr), trace.String(trace.TagComment, ""))
	//	defer func() {
	//		if err != nil {
	//			t.Finish(&err)
	//		}
	//	}()
	//}
	if _, err = db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(db.conf.Addr, db.conf.Addr, "begin", "breaker")
		return
	}
	_, c, cancel := db.conf.tranTimeout.Shrink(c)
	rtx, err := db.BeginTx(c, nil)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), db.conf.Addr, db.conf.Addr, "begin")
	if err != nil {
		err = errors.WithStack(err)
		cancel()
		return
	}
	// tx = &Tx{tx: rtx, t: t, db: db, c: c, cancel: cancel}
	tx = &Tx{tx: rtx, db: db, c: c, cancel: cancel}
	return
}

func (db *conn) exec(c context.Context, query string, args ...interface{}) (res sql.Result, err error) {
	now := time.Now()
	defer slowLog(fmt.Sprintf("Exec query(%s) args(%+v)", query, args), now)
	//if t, ok := trace.FromContext(c); ok {
	//	t = t.Fork(_family, "exec")
	//	t.SetTag(trace.String(trace.TagAddress, db.conf.Addr), trace.String(trace.TagComment, query))
	//	defer t.Finish(&err)
	//}
	if _, err = db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(db.conf.Addr, db.conf.Addr, "exec", "breaker")
		return
	}
	_, c, cancel := db.conf.execTimeout.Shrink(c)
	res, err = db.ExecContext(c, query, args...)
	cancel()
	db.onBreaker(&err)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), db.conf.Addr, db.conf.Addr, "exec")
	if err != nil {
		err = errors.Wrapf(err, "exec:%s, args:%+v", query, args)
	}
	return
}

func (db *conn) ping(c context.Context) (err error) {
	now := time.Now()
	defer slowLog("Ping", now)
	//if t, ok := trace.FromContext(c); ok {
	//	t = t.Fork(_family, "ping")
	//	t.SetTag(trace.String(trace.TagAddress, db.conf.Addr), trace.String(trace.TagComment, ""))
	//	defer t.Finish(&err)
	//}
	if _, err = db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(db.conf.Addr, db.conf.Addr, "ping", "breaker")
		return
	}
	_, c, cancel := db.conf.execTimeout.Shrink(c)
	err = db.PingContext(c)
	cancel()
	db.onBreaker(&err)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), db.conf.Addr, db.conf.Addr, "ping")
	if err != nil {
		err = errors.WithStack(err)
	}
	return
}

func (db *conn) prepare(query string) (*Stmt, error) {
	defer slowLog(fmt.Sprintf("Prepare query(%s)", query), time.Now())
	stmt, err := db.Prepare(query)
	if err != nil {
		err = errors.Wrapf(err, "prepare %s", query)
		return nil, err
	}
	st := &Stmt{query: query, db: db}
	st.stmt.Store(stmt)
	return st, nil
}

func (db *conn) prepared(query string) (stmt *Stmt) {
	defer slowLog(fmt.Sprintf("Prepared query(%s)", query), time.Now())
	stmt = &Stmt{query: query, db: db}
	s, err := db.Prepare(query)
	if err == nil {
		stmt.stmt.Store(s)
		return
	}
	go func() {
		for {
			s, err := db.Prepare(query)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			stmt.stmt.Store(s)
			return
		}
	}()
	return
}

func (db *conn) query(c context.Context, query string, args ...interface{}) (rows *Rows, err error) {
	now := time.Now()
	defer slowLog(fmt.Sprintf("Query query(%s) args(%+v)", query, args), now)
	//if t, ok := trace.FromContext(c); ok {
	//	t = t.Fork(_family, "query")
	//	t.SetTag(trace.String(trace.TagAddress, db.conf.Addr), trace.String(trace.TagComment, query))
	//	defer t.Finish(&err)
	//}
	if _, err = db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(db.conf.Addr, db.conf.Addr, "query", "breaker")
		return
	}
	_, c, cancel := db.conf.queryTimeout.Shrink(c)
	rs, err := db.DB.QueryContext(c, query, args...)
	db.onBreaker(&err)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), db.conf.Addr, db.conf.Addr, "query")
	if err != nil {
		err = errors.Wrapf(err, "query:%s, args:%+v", query, args)
		cancel()
		return
	}
	rows = &Rows{Rows: rs, cancel: cancel, Mapper: db.Mapper}
	return
}

func (db *conn) queryRow(c context.Context, query string, args ...interface{}) *Row {
	now := time.Now()
	defer slowLog(fmt.Sprintf("QueryRow query(%s) args(%+v)", query, args), now)

	// t, ok := trace.FromContext(c)
	//if ok {
	//	t = t.Fork(_family, "queryrow")
	//	t.SetTag(trace.String(trace.TagAddress, db.conf.Addr), trace.String(trace.TagComment, query))
	//}
	if _, err := db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(db.conf.Addr, db.conf.Addr, "queryRow", "breaker")
		// return &Row{db: db, t: t, err: err}
		return &Row{db: db, err: err}
	}
	_, c, cancel := db.conf.queryTimeout.Shrink(c)
	rows, err := db.DB.QueryContext(c, query, args...)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), db.conf.Addr, db.conf.Addr, "queryrow")
	// return &Row{db: db, rows: rows, err: err, query: query, args: args, t: t, cancel: cancel, Mapper: db.Mapper}
	return &Row{db: db, rows: rows, err: err, query: query, args: args, cancel: cancel, Mapper: db.Mapper}
}

// Select using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *conn) _select(c context.Context, dest interface{}, query string, args ...interface{}) error {
	rows, err := db.query(c, query, args...)
	if err != nil {
		return err
	}
	// if something happens here, we want to make sure the rows are Closed
	defer rows.Close()
	return scanAll(rows, dest, false)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *conn) _get(c context.Context, dest interface{}, query string, args ...interface{}) error {
	r := db.queryRow(c, query, args...)
	return r.ScanAll(dest)
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *conn) namedQuery(c context.Context, query string, arg interface{}) (*Rows, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return nil, err
	}
	return db.query(c, q, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *conn) namedExec(c context.Context, query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, db.Mapper)
	if err != nil {
		return nil, err
	}
	return db.exec(c, q, args...)

}

// Close closes the statement.
func (s *Stmt) Close() (err error) {
	if s == nil {
		err = ErrStmtNil
		return
	}
	stmt, ok := s.stmt.Load().(*sql.Stmt)
	if ok {
		err = errors.WithStack(stmt.Close())
	}
	return
}

// Exec executes a prepared statement with the given arguments and returns a
// Result summarizing the effect of the statement.
func (s *Stmt) Exec(c context.Context, args ...interface{}) (res sql.Result, err error) {
	if s == nil {
		err = ErrStmtNil
		return
	}
	now := time.Now()
	defer slowLog(fmt.Sprintf("Exec query(%s) args(%+v)", s.query, args), now)
	//if s.tx {
	//	if s.t != nil {
	//		s.t.SetTag(trace.String(trace.TagAnnotation, s.query))
	//	}
	//} else if t, ok := trace.FromContext(c); ok {
	//	t = t.Fork(_family, "exec")
	//	t.SetTag(trace.String(trace.TagAddress, s.db.conf.Addr), trace.String(trace.TagComment, s.query))
	//	defer t.Finish(&err)
	//}
	if _, err = s.db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(s.db.conf.Addr, s.db.conf.Addr, "stmt:exec", "breaker")
		return
	}
	stmt, ok := s.stmt.Load().(*sql.Stmt)
	if !ok {
		err = ErrStmtNil
		return
	}
	_, c, cancel := s.db.conf.execTimeout.Shrink(c)
	res, err = stmt.ExecContext(c, args...)
	cancel()
	s.db.onBreaker(&err)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), s.db.conf.Addr, s.db.conf.Addr, "stmt:exec")
	if err != nil {
		err = errors.Wrapf(err, "exec:%s, args:%+v", s.query, args)
	}
	return
}

// Query executes a prepared query statement with the given arguments and
// returns the query results as a *Rows.
func (s *Stmt) Query(c context.Context, args ...interface{}) (rows *Rows, err error) {
	if s == nil {
		err = ErrStmtNil
		return
	}
	now := time.Now()
	defer slowLog(fmt.Sprintf("Query query(%s) args(%+v)", s.query, args), now)
	//if s.tx {
	//	if s.t != nil {
	//		s.t.SetTag(trace.String(trace.TagAnnotation, s.query))
	//	}
	//} else if t, ok := trace.FromContext(c); ok {
	//	t = t.Fork(_family, "query")
	//	t.SetTag(trace.String(trace.TagAddress, s.db.conf.Addr), trace.String(trace.TagComment, s.query))
	//	defer t.Finish(&err)
	//}
	if _, err = s.db.breaker.Allow(); err != nil {
		_metricReqErr.Inc(s.db.conf.Addr, s.db.conf.Addr, "stmt:query", "breaker")
		return
	}
	stmt, ok := s.stmt.Load().(*sql.Stmt)
	if !ok {
		err = ErrStmtNil
		return
	}
	_, c, cancel := s.db.conf.queryTimeout.Shrink(c)
	rs, err := stmt.QueryContext(c, args...)
	s.db.onBreaker(&err)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), s.db.conf.Addr, s.db.conf.Addr, "stmt:query")
	if err != nil {
		err = errors.Wrapf(err, "query:%s, args:%+v", s.query, args)
		cancel()
		return
	}
	rows = &Rows{Rows: rs, cancel: cancel, Mapper: s.db.Mapper}
	return
}

// QueryRow executes a prepared query statement with the given arguments.
// If an error occurs during the execution of the statement, that error will
// be returned by a call to Scan on the returned *Row, which is always non-nil.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
func (s *Stmt) QueryRow(c context.Context, args ...interface{}) (row *Row) {
	now := time.Now()
	defer slowLog(fmt.Sprintf("QueryRow query(%s) args(%+v)", s.query, args), now)
	row = &Row{db: s.db, query: s.query, args: args}
	if s == nil {
		row.err = ErrStmtNil
		return
	}
	//if s.tx {
	//	if s.t != nil {
	//		s.t.SetTag(trace.String(trace.TagAnnotation, s.query))
	//	}
	//} else if t, ok := trace.FromContext(c); ok {
	//	t = t.Fork(_family, "queryrow")
	//	t.SetTag(trace.String(trace.TagAddress, s.db.conf.Addr), trace.String(trace.TagComment, s.query))
	//	row.t = t
	//}
	if _, row.err = s.db.breaker.Allow(); row.err != nil {
		_metricReqErr.Inc(s.db.conf.Addr, s.db.conf.Addr, "stmt:queryrow", "breaker")
		return
	}
	stmt, ok := s.stmt.Load().(*sql.Stmt)
	if !ok {
		return
	}
	_, c, cancel := s.db.conf.queryTimeout.Shrink(c)
	row.rows, row.err = stmt.QueryContext(c, args...)
	row.cancel = cancel
	row.Mapper = s.db.Mapper
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), s.db.conf.Addr, s.db.conf.Addr, "stmt:queryrow")
	return
}

func (tx *Tx) Context() context.Context {
	return tx.c
}

// Commit commits the transaction.
func (tx *Tx) Commit() (err error) {
	err = tx.tx.Commit()
	tx.cancel()
	tx.db.onBreaker(&err)
	//if tx.t != nil {
	//	//tx.t.Finish(&err)
	//	tx.t.Finish()
	//}
	if err != nil {
		err = errors.WithStack(err)
	}
	return
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() (err error) {
	err = tx.tx.Rollback()
	tx.cancel()
	tx.db.onBreaker(&err)
	//if tx.t != nil {
	//	// tx.t.Finish(&err)
	//	tx.t.Finish()
	//}
	if err != nil {
		err = errors.WithStack(err)
	}
	return
}

// Exec executes a query that doesn't return rows. For example: an INSERT and
// UPDATE.
func (tx *Tx) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	now := time.Now()
	defer slowLog(fmt.Sprintf("Exec query(%s) args(%+v)", query, args), now)
	//if tx.t != nil {
	//	tx.t.SetTag(trace.String(trace.TagAnnotation, fmt.Sprintf("exec %s", query)))
	//}
	res, err = tx.tx.ExecContext(tx.c, query, args...)
	_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), tx.db.conf.Addr, tx.db.conf.Addr, "tx:exec")
	if err != nil {
		err = errors.Wrapf(err, "exec:%s, args:%+v", query, args)
	}
	return
}

// Query executes a query that returns rows, typically a SELECT.
func (tx *Tx) Query(query string, args ...interface{}) (rows *Rows, err error) {
	//if tx.t != nil {
	//	tx.t.SetTag(trace.String(trace.TagAnnotation, fmt.Sprintf("query %s", query)))
	//}
	now := time.Now()
	defer slowLog(fmt.Sprintf("Query query(%s) args(%+v)", query, args), now)
	defer func() {
		_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), tx.db.conf.Addr, tx.db.conf.Addr, "tx:query")
	}()
	rs, err := tx.tx.QueryContext(tx.c, query, args...)
	if err == nil {
		rows = &Rows{Rows: rs, Mapper: tx.db.Mapper}
	} else {
		err = errors.Wrapf(err, "query:%s, args:%+v", query, args)
	}
	return
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedQuery(query string, arg interface{}) (*Rows, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.db.Mapper)
	if err != nil {
		return nil, err
	}
	return tx.Query(q, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.db.Mapper)
	if err != nil {
		return nil, err
	}
	return tx.Exec(q, args...)

}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until Row's
// Scan method is called.
func (tx *Tx) QueryRow(query string, args ...interface{}) *Row {
	//if tx.t != nil {
	//	tx.t.SetTag(trace.String(trace.TagAnnotation, fmt.Sprintf("queryrow %s", query)))
	//}
	now := time.Now()
	defer slowLog(fmt.Sprintf("QueryRow query(%s) args(%+v)", query, args), now)
	defer func() {
		_metricReqDur.Observe(int64(time.Since(now)/time.Millisecond), tx.db.conf.Addr, tx.db.conf.Addr, "tx:queryrow")
	}()
	rows, err := tx.tx.QueryContext(tx.c, query, args...)
	return &Row{rows: rows, err: err, db: tx.db, query: query, args: args, Mapper: tx.db.Mapper}
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
func (tx *Tx) Stmt(stmt *Stmt) *Stmt {
	as, ok := stmt.stmt.Load().(*sql.Stmt)
	if !ok {
		return nil
	}
	ts := tx.tx.StmtContext(tx.c, as)
	// st := &Stmt{query: stmt.query, tx: true, t: tx.t, db: tx.db}
	st := &Stmt{query: stmt.query, tx: true, db: tx.db}
	st.stmt.Store(ts)
	return st
}

// Prepare creates a prepared statement for use within a transaction.
// The returned statement operates within the transaction and can no longer be
// used once the transaction has been committed or rolled back.
// To use an existing prepared statement on this transaction, see Tx.Stmt.
func (tx *Tx) Prepare(query string) (*Stmt, error) {
	//if tx.t != nil {
	//	tx.t.SetTag(trace.String(trace.TagAnnotation, fmt.Sprintf("prepare %s", query)))
	//}
	defer slowLog(fmt.Sprintf("Prepare query(%s)", query), time.Now())
	stmt, err := tx.tx.Prepare(query)
	if err != nil {
		err = errors.Wrapf(err, "prepare %s", query)
		return nil, err
	}
	// st := &Stmt{query: query, tx: true, t: tx.t, db: tx.db}
	st := &Stmt{query: query, tx: true, db: tx.db}
	st.stmt.Store(stmt)
	return st, nil
}

// parseDSNAddr parse dsn name and return addr.
func parseDSNAddr(dsn string) (addr string) {
	if dsn == "" {
		return
	}
	part0 := strings.Split(dsn, "@")
	if len(part0) > 1 {
		part1 := strings.Split(part0[1], "?")
		if len(part1) > 0 {
			addr = part1[0]
		}
	}
	return
}

func slowLog(statement string, now time.Time) {
	du := time.Since(now)
	if du > _slowLogDuration {
		logx.WithDuration(du).Slowf("%s slow log statement: %s time: %v", _family, statement, du)
	}
}
