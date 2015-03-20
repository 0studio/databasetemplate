package databasetemplate

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"testing"
)

type Test struct {
	A int
	B string
}

func TestExec(t *testing.T) {
	db, err := sql.Open("mysql", "th_dev:th_devpass@/test?charset=utf8")
	defer db.Close()
	db.SetMaxIdleConns(10)
	if err != nil {
		return
	}
	dbTemplate := &DatabaseTemplateImpl{db}
	err = dbTemplate.Exec(nil, "create table if not exists test(a int not null AUTO_INCREMENT,b varchar(10),primary key(a))")
	if err != nil {
		t.Error("can't get from db", err)
	}

	err = dbTemplate.Exec(nil, "truncate table test")
	if err != nil {
		t.Error("can't get from db", err)
	}

	err = dbTemplate.Exec(nil, "insert into  test(b) values(?)", "aaa")
	if err != nil {
		t.Error("can't get from db", err)
	}

	err = dbTemplate.Exec(nil, "insert into  test(b) values(?)", "bbb")
	if err != nil {
		t.Error("can't get from db", err)
	}
	err = dbTemplate.Exec(nil, "truncate table test")
	if err != nil {
		t.Error("can't get from db", err)
	}

	// mapRow := func(resultSet *sql.Rows) (object interface{}, err error) {
	// 	t := Test{}
	// 	err = resultSet.Scan(&t.A, &t.B)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return t, err
	// }

	// mapRowPtr := func(resultSet *sql.Rows) (object interface{}, err error) {
	// 	t := Test{}
	// 	err = resultSet.Scan(&t.A, &t.B)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return &t, err
	// }

	// list, err := dbTemplate.Query("select a,b,c from test", nil, mapRow)
	// if list != nil {
	// 	t.Error("list should be nil", list)
	// }
	// if err.Error() != "Error 1054: Unknown column 'c' in 'field list'" {
	// 	t.Errorf("error should be Error 1054: Unknown column 'c' in 'field list'")
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
