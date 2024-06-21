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
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	ErrNotFound = sql.ErrNoRows
)

// Config mysql config.
type Config struct {
	DSN            string   // write data source name.
	ReadDSN        []string `json:",optional"` // read data source name.
	Active         int      `json:",optional"` // pool
	Idle           int      `json:",optional"` // pool
	IdleTimeout    string   `json:",optional"` // connect max life time.
	DisableStmtLog bool     `json:",default=false"`
	SlowThreshold  int64    `json:",default=500"`
}

// NewMySQL new db and retry connection when has error.
func NewMySQL(c *Config) (db *DB) {
	db, err := Open(c)
	if err != nil {
		logx.Error("open mysql error(%v)", err)
		panic(err)
	}

	if c.DisableStmtLog {
		sqlx.DisableStmtLog()
	}

	if c.SlowThreshold > 500 {
		sqlx.SetSlowThreshold(time.Duration(c.SlowThreshold) * time.Millisecond)
	}

	return
}

//// TxWrapper TxWrapper
//func TxWrapper(ctx context.Context, db *DB, fn func(*Tx) error) error {
//	return db.write.Transact(ctx, fn)
//}

// TxWrapper TxWrapper
func TxWrapper(ctx context.Context, db *DB, txF func(*Tx, *StoreResult)) *StoreResult {
	result := &StoreResult{}

	result.Err = db.write.Transact(ctx, func(tx *Tx) error {
		txF(tx, result)
		return result.Err
	})

	return result
}

// IsDuplicate
// Check if MySQL error is a Error Code: 1062. Duplicate entry ... for key ...
func IsDuplicate(err error) bool {
	if err == nil {
		return false
	}

	var (
		mysqlErr *mysql.MySQLError
	)

	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

// IsMissingDb
// Check if MySQL error is a Error Code: 1049. Unknown database ...
func IsMissingDb(err error) bool {
	if err == nil {
		return false
	}

	var (
		mysqlErr *mysql.MySQLError
	)

	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1049
}
