package databasetemplate

import (
	"database/sql"
	"fmt"
)

type DBConfig struct {
	Host string
	User string
	Pass string
	Name string
}

func NewDBInstance(dbConfig DBConfig, debug bool) (db *sql.DB, ok bool) {
	var (
		dbToken string
		err     error
		Log     string
	)

	dbToken = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=true&loc=Local&tls=false&timeout=1m", dbConfig.User, dbConfig.Pass, dbConfig.Host, dbConfig.Name)
	Log = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=true&loc=Local&tls=false&timeout=1m\n", dbConfig.User, "password", dbConfig.Host, dbConfig.Name)
	db, err = sql.Open("mysql", dbToken)
	if err != nil {
		fmt.Println("error", Log, err)
		ok = false
		return
	}

	err = db.Ping()
	if err != nil {
		fmt.Println("error", Log, err)
		ok = false
		return

	}
	if debug {
		fmt.Print(Log)
	}
	ok = true
	return

}
