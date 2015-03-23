package databasetemplate

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type DBConfig struct {
	Host string
	User string
	Pass string
	Name string
}

func NewDatabaseTemplateWithConfig(dbConfig DBConfig, debug bool) (dt DatabaseTemplate, ok bool) {
	var db *sql.DB
	db, ok = NewDBInstance(dbConfig, debug)
	if !ok {
		return
	}
	return &DatabaseTemplateImpl{db}, ok

}
func NewDatabaseTemplateShardingWithConfig(dbConfig DBConfig, splitDBCount int, debug bool) (splitDT DatabaseTemplate, ok bool) {
	var dtList []DatabaseTemplate = make([]DatabaseTemplate, splitDBCount)
	dbNamePrefix := dbConfig.Name

	for i := 0; i < splitDBCount; i++ {
		dbConfig.Name = fmt.Sprintf("%s_%d", dbNamePrefix, i)
		dtList[i], ok = NewDatabaseTemplateWithConfig(dbConfig, debug)
		if !ok {
			return
		}
	}
	return &DatabaseTemplateImplShardingImpl{dtList}, true
}
func NewDatabaseTemplate(db *sql.DB) (dt DatabaseTemplate) {
	return &DatabaseTemplateImpl{db}
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
