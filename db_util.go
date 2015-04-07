package databasetemplate

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type DBConfig struct {
	Host string `json:"host,omitempty"`
	User string `json:"user,omitempty"`
	Pass string `json:"passwd,omitempty"`
	Name string `json:"database,omitempty"`
	Port string `json:"port,omitempty"`
}
type MasterSlaveConfig struct {
	Master    DBConfig   `json:"master,omitempty"`
	SlaveList []DBConfig `json:"slave,omitempty"`
}

func (config MasterSlaveConfig) SlaveListLength() int {
	return len(config.SlaveList)
}

type ShardingConfig struct {
	MasterList       []MasterSlaveConfig `json:"sharding,omitempty"`
	MasterListLength int                 `json:"sharding_length,omitempty"`
}

func ParseMasterSlaveConfig(jsonString string) (masterSlaveConfig MasterSlaveConfig, ok bool) {
	err := json.Unmarshal([]byte(jsonString), &masterSlaveConfig)
	if err != nil {
		fmt.Println("parse_master_slave_config_json_error", err)
		return
	}

	ok = true
	return

}
func ParseShardingConfig(jsonString string) (shardingConfig ShardingConfig, ok bool) {
	err := json.Unmarshal([]byte(jsonString), &shardingConfig)
	if err != nil {
		fmt.Println("parse_shardingConfig_json_error", err)
		return
	}
	// 用来保证 用来做sharding 的mysql 个数与配置的个数相同， 避免在连接mysql的过程中有连接失败导致sharding 不一致
	ok = shardingConfig.MasterListLength == len(shardingConfig.MasterList)
	return

}

func NewDatabaseTemplateShardingWithConfig(shardingConfig ShardingConfig, debug bool) (shardingDT DatabaseTemplate, ok bool) {
	var dtList []DatabaseTemplate = make([]DatabaseTemplate, len(shardingConfig.MasterList))

	for i := 0; i < len(shardingConfig.MasterList); i++ {
		if shardingConfig.MasterList[i].SlaveListLength() == 0 {
			dtList[i], ok = NewDatabaseTemplateWithConfig(shardingConfig.MasterList[i].Master, debug)
			if !ok {
				return
			}
		} else {
			dtList[i], ok = NewRWDatabaseTemplateWithConfig(shardingConfig.MasterList[i], debug)
			if !ok {
				return
			}

		}

	}
	return &DatabaseTemplateImplShardingImpl{dtList}, true
}
func NewRWDatabaseTemplateWithConfig(config MasterSlaveConfig, debug bool) (dt DatabaseTemplate, ok bool) {
	var masterDT DatabaseTemplate
	var slaveDTList []DatabaseTemplate = make([]DatabaseTemplate, config.SlaveListLength())
	masterDT, ok = NewDatabaseTemplateWithConfig(config.Master, debug)
	if !ok {
		return
	}
	for idx, slaveConfig := range config.SlaveList {
		slaveDTList[idx], ok = NewDatabaseTemplateWithConfig(slaveConfig, debug)
		if !ok {
			fmt.Println("conn_to_mysql_slave_err", slaveConfig)
			return
		}

	}

	dt = NewRWDatabaseTemplate(masterDT, slaveDTList)
	return
}

func NewDatabaseTemplateWithConfig(dbConfig DBConfig, debug bool) (dt DatabaseTemplate, ok bool) {
	var db *sql.DB
	db, ok = NewDBInstance(dbConfig, debug)
	if !ok {
		return
	}
	return &DatabaseTemplateImpl{db}, ok

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

	if dbConfig.Port == "" {
		dbConfig.Port = "3306"
	}

	dbToken = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true&loc=Local&tls=false&timeout=1m", dbConfig.User, dbConfig.Pass, dbConfig.Host, dbConfig.Port, dbConfig.Name)
	Log = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true&loc=Local&tls=false&timeout=1m\n", dbConfig.User, "password", dbConfig.Host, dbConfig.Port, dbConfig.Name)
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
