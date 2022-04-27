// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package sqlx

import (
	"context"
	"database/sql"

	"github.com/teamgram/marmota/pkg/stores/sqlx/reflectx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type Tx struct {
	conn sqlx.Session
	*reflectx.Mapper
	ctx context.Context
}

func newTx(sess sqlx.Session) *Tx {
	return &Tx{
		conn:   sess,
		Mapper: mapper(),
	}
}

func (tx *Tx) Context() context.Context {
	return tx.ctx
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.conn.ExecCtx(tx.ctx, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return nil, err
	}

	return tx.conn.ExecCtx(tx.ctx, q, args...)
}

func (tx *Tx) Prepare(query string) (sqlx.StmtSession, error) {
	return tx.conn.PrepareCtx(tx.ctx, query)
}

func (tx *Tx) QueryRow(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowCtx(tx.ctx, v, query, args...)
}

// NamedQueryRow using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedQueryRow(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRow(v, q, args...)
}

func (tx *Tx) QueryRowPartial(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowPartialCtx(tx.ctx, v, query, args...)
}

func (tx *Tx) NamedQueryRowPartial(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRowPartial(v, q, args...)
}

func (tx *Tx) QueryRows(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowsCtx(tx.ctx, v, query, args...)
}

func (tx *Tx) NamedQueryRows(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRows(v, q, args...)
}

func (tx *Tx) QueryRowsPartial(v interface{}, query string, args ...interface{}) error {
	return tx.conn.QueryRowsPartialCtx(tx.ctx, v, query, args...)
}

func (tx *Tx) NamedQueryRowsPartial(v interface{}, query string, arg interface{}) error {
	q, args, err := bindNamedMapper(QUESTION, query, arg, tx.Mapper)
	if err != nil {
		return err
	}
	return tx.QueryRowsPartial(v, q, args...)
}
