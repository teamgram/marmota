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

package main

import (
	"context"
	"fmt"

	"github.com/teamgram/marmota/pkg/stores/sqlx"

	"github.com/zeromicro/go-zero/core/logx"
)

//var configData = `
//[demo]
//	addr = "127.0.0.1:3306"
//	dsn = "root:@tcp(127.0.0.1:3306)/test2?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
//	readDSN = ["root:@tcp(127.0.0.1:3306)/test2?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8","root:@tcp(127.0.0.1:3306)/test2?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8,utf8mb4"]
//	active = 64
//	idle = 64
//	idleTimeout ="6m"
//	queryTimeout = "5s"
//	execTimeout = "5s"
//	tranTimeout = "5s"
//`

type Message1DO struct {
	Id        int32  `db:"id"`
	MessageId int64  `db:"message_id"`
	Data2     string `db:"data2"`
	State     int32  `db:"state"`
	Deleted   bool   `db:"deleted"`
	CreatedAt string `db:"created_at"`
}

func main() {
	c := &sqlx.Config{
		// Addr:         "127.0.0.1:3306",
		DSN:         "root:@tcp(127.0.0.1:3306)/test2?charset=utf8mb4&parseTime=true&loc=Asia%2FShanghai",
		ReadDSN:     nil,
		Active:      64,
		Idle:        64,
		IdleTimeout: "4h",
		//QueryTimeout: "5s",
		//ExecTimeout:  "5s",
		//TranTimeout:  "5s",
	}
	db := sqlx.NewMySQL(c)
	//// defer db.Close()
	//commonDAO := sqlx.NewCommonDAO(db)
	//
	//existed := commonDAO.CheckExists(
	//	context.Background(),
	//	"message1",
	//	map[string]interface{}{
	//		"id": 1,
	//	})
	//fmt.Println("id: 1 ==> ", existed)
	//
	//existed = commonDAO.CheckExists(
	//	context.Background(),
	//	"message1",
	//	map[string]interface{}{
	//		"id": 10,
	//	})
	//fmt.Println("id: 10 ==> ", existed)
	//
	//sz := commonDAO.CalcSize(
	//	context.Background(),
	//	"message1",
	//	map[string]interface{}{
	//		"deleted": 1,
	//	})
	//fmt.Println("sz ==> ", sz)
	//
	//sz = commonDAO.CalcSizeByWhere(
	//	context.Background(),
	//	"message1",
	//	"deleted = 1")
	//
	//fmt.Println("sz ==> ", sz)
	//
	//existed, _ = sqlx.CheckExists(
	//	context.Background(),
	//	commonDAO.DB(),
	//	"message1",
	//	map[string]interface{}{
	//		"id": 10,
	//	})
	//fmt.Println("id: 10 ==> ", existed)

	// NamedExec2(db)

	//Query(db)
	//QueryRow(db)
	//Get(db)
	//Select(db)
	//In(db)
	// Tx(db)
	// TxNamedExec(db)
	BulkExec(db)
}

func Query(db *sqlx.DB) {
	var (
		v []Message1DO
	)

	err := db.QueryRows(context.Background(), &v, "SELECT * FROM message1 WHERE id = 0")
	if err != nil {
		logx.Errorf("query error - %v", err)
		return
	}

	logx.Info("%+v", v)
}

func QueryRow(db *sqlx.DB) {
	var (
		v1 = new(Message1DO)
	)

	err := db.QueryRow(context.Background(), v1, "SELECT * FROM message1 LIMIT 1")
	if err != nil {
		logx.Errorf("query error - %v", err)
		// return
	}
	fmt.Println(v1)

	err = db.QueryRow(context.Background(), v1, "SELECT * FROM message1 WHERE id = 10")
	if err != nil && err != sqlx.ErrNoRows {
		logx.Errorf("query error - %v", err)
		// return
	}

	fmt.Println(v1)
}

func Get(db *sqlx.DB) {
	v1 := Message1DO{}
	if err := db.QueryRow(context.Background(), &v1, "SELECT * FROM message1 LIMIT 1"); err != nil {
		logx.Errorf("query error - %v", err)
		return
	}
	logx.Infof("%#v", v1)
}

func Select(db *sqlx.DB) {
	var (
		v1 []Message1DO
	)

	if err := db.QueryRows(context.Background(), &v1, "SELECT * FROM message1"); err != nil {
		logx.Error("query error - %v", err)
		return
	}
	logx.Infof("%#v", v1)
}

func In(db *sqlx.DB) {
	var (
		err error

		a     = make([]interface{}, 0)
		query = "SELECT * FROM message1 WHERE id IN (?)"
		v1    = make([]Message1DO, 0)
	)

	query, a, err = sqlx.In(query, []int32{1, 8, 15})
	if err != nil {
		// r sql.Result
		logx.Errorf("sqlx.In in SelectListByPhoneList(_), error: %v", err)
		return
	}

	if err = db.QueryRows(context.Background(), &v1, query, a...); err != nil {
		logx.Errorf("query error - %v", err)
		return
	}

	for i := 0; i < len(v1); i++ {
		logx.Infof("%+v", v1[i])
	}
}

func Tx(db *sqlx.DB) {
	sqlx.TxWrapper(db, context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
		tx.Exec(ctx, "INSERT INTO message1 (message_id, data2, state, deleted) VALUES(1000, '1000', 0, 0)")
		return nil
	})
	//db.Master().
	//tx, err := db.Begin(context.Background())
	//if err != nil {
	//	logx.Errorf("Begin error - %v", err)
	//	return
	//}
	//
	//_, err = tx.Exec("INSERT INTO message1 (message_id, data2, state) VALUES(1000, '1000', 0)")
	//if err != nil {
	//	logx.Errorf("exec error - %v", err)
	//	tx.Rollback()
	//} else {
	//	tx.Commit()
	//}
}

//func Stmt(db *sqlx.DB) {
//	v1 := Message1DO{}
//
//	stmt, err := db.Prepare("SELECT * FROM message1")
//	if err != nil {
//		logx.Errorf("Begin error - %v", err)
//		return
//	}
//
//	defer stmt.Close()
//
//	err = stmt.QueryRow(context.Background()).StructScan(&v1)
//	// "INSERT INTO message1 (message_id, data2, state) VALUES(1000, '1000', 0)")
//	if err != nil {
//		logx.Errorf("exec error - %v", err)
//		return
//	}
//
//	logx.Infof("%+v", v1)
//}
//
//func NamedExec(db *sqlx.DB) {
//	v1 := &Message1DO{
//		MessageId: 1003,
//		Data2:     "1003",
//		State:     0,
//	}
//
//	_, err := db.NamedExec(context.Background(), "INSERT INTO message1 (message_id, data2, state) VALUES(:message_id, :data2, :state)", v1)
//	if err != nil {
//		logx.Errorf("exec error - %v", err)
//	}
//}

func NamedExec2(db *sqlx.DB) {
	v1 := &Message1DO{
		MessageId: 2005,
		Data2:     "2005",
		State:     0,
		Deleted:   true,
	}

	_, err := db.NamedExec(context.Background(), "INSERT INTO message1 (message_id, data2, state, deleted) VALUES(:message_id, :data2, :state, :deleted)", v1)
	if err != nil {
		logx.Errorf("exec error - %v", err)
	}
}

func TxNamedExec(db *sqlx.DB) {
	v1 := &Message1DO{
		MessageId: 1005,
		Data2:     "1004",
		State:     1,
		Deleted:   false,
	}

	sqlx.TxWrapper(db, context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
		_, err := tx.NamedExec(
			ctx,
			"INSERT INTO message1 (message_id, data2, state, deleted) VALUES(:message_id, :data2, :state, :deleted)",
			v1)
		if err != nil {
			fmt.Println(err)
		}
		return err
	})
}

func BulkExec(db *sqlx.DB) {
	v1 := []*Message1DO{
		&Message1DO{
			MessageId: 1104,
			Data2:     "1004",
			State:     0,
			Deleted:   false,
		},
		&Message1DO{
			MessageId: 1105,
			Data2:     "1005",
			State:     0,
			Deleted:   false,
		},
	}

	// sqlx2.NewBulkInserter(db.Master(), "INSERT INTO message1 (message_id, data2, state) VALUES(:message_id, :data2, :state)")
	_, err := db.NamedExec(
		context.Background(),
		"INSERT INTO message1 (message_id, data2, state, deleted) VALUES(:message_id, :data2, :state, :deleted)",
		v1)
	if err != nil {
		logx.Error("exec error - %v", err)
	}
}
