package databasetemplate

import (
	"database/sql"
	"fmt"
	key "github.com/0studio/storage_key"
	_ "github.com/go-sql-driver/mysql"
	"testing"
)

func TestExecSharding(t *testing.T) {
	db, err := sql.Open("mysql", "th_dev:th_devpass@/test?charset=utf8")
	defer db.Close()
	db.SetMaxIdleConns(10)
	if err != nil {
		return
	}
	db2, err := sql.Open("mysql", "th_dev:th_devpass@/test_2?charset=utf8")
	defer db2.Close()
	db2.SetMaxIdleConns(10)
	if err != nil {
		return
	}
	dbTemplate := NewDatabaseTemplateSharding([]*sql.DB{db, db2})

	err = dbTemplate.ExecDDL("drop table if exists test")
	if err != nil {
		t.Error("can't get from db", err)
	}
	err = dbTemplate.ExecDDL("create table if not exists test(a int not null ,b varchar(10),primary key(a))")
	if err != nil {
		t.Error("can't get from db", err)
	}

	err = dbTemplate.Exec(nil, "truncate table test")
	if err != nil {
		t.Error("can't get from db", err)
	}

	err = dbTemplate.Exec(key.KeyUint64(1), "insert into  test(a,b) values(?,?)", 1, "aaa")
	if err != nil {
		t.Error("can't get from db", err)
	}

	err = dbTemplate.Exec(key.KeyUint64(2), "insert into  test(a,b) values(?,?)", 2, "bbb")
	if err != nil {
		t.Error("can't get from db", err)
	}

	mapRow := func(resultSet *sql.Rows) (object interface{}, err error) {
		t := Test{}
		err = resultSet.Scan(&t.A, &t.B)
		if err != nil {
			return nil, err
		}
		return t, err
	}
	obj, err := dbTemplate.QueryObject(key.KeyUint64(1), "select a,b from test where a=?", mapRow, 1)
	if err != nil {
		t.Error("obj should be nil", obj)
	}
	fmt.Println(obj)
	obj, err = dbTemplate.QueryObject(key.KeyUint64(2), "select a,b from test where a=?", mapRow, 2)
	if err != nil {
		t.Error("obj should be nil", obj)
	}
	fmt.Println(obj)

	list, err := dbTemplate.QueryArray(key.KeyUint64List{key.KeyUint64(1), key.KeyUint64(2)}, "select a,b from test where a in (1,2)", mapRow)
	if err != nil {
		t.Error("obj should be nil", obj)
	}
	if len(list) != 2 {
		t.Error("list should return 2 elements", list)
	}

	// mapRowPtr := func(resultSet *sql.Rows) (object interface{}, err error) {
	// 	t := Test{}
	// 	err = resultSet.Scan(&t.A, &t.B)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return &t, err
	// }

	// obj, err := dbTemplate.Query("select a,b,c from test", nil, mapRow)
	// if obj != nil {
	// 	t.Error("obj should be nil", obj)
	// }
	// if err.Error() != "Error 1054: Unknown column 'c' in 'field obj'" {
	// 	t.Errorf("error should be Error 1054: Unknown column 'c' in 'field obj'")
	// }
	// var tests []Test
	// err = dbTemplate.QueryIntoArray(&tests, "select * from test", mapRow)
	// if err != nil {
	// 	t.Error("can't get from db", err)
	// }
	// fmt.Println(tests)

	// var testsPtr []*Test
	// err = dbTemplate.QueryIntoArray(&testsPtr, "select * from test", mapRowPtr)
	// if err != nil {
	// 	t.Error("can't get from db", err)
	// }
	// fmt.Println(*testsPtr[0])

	// var testsPtr2 []*Test
	// err = dbTemplate.QueryIntoArray(&testsPtr2, "select * from test", mapRow)
	// if err == nil {
	// 	t.Error("should be error")
	// }

	// var tests2 []Test
	// err = dbTemplate.QueryIntoArray(&tests2, "select * from test", mapRowPtr)
	// if err == nil {
	// 	t.Error("should be error")
	// }

}
