package databasetemplate

import (
	"database/sql"
	"github.com/0studio/storage_key"
)

type DatabaseTemplateRWImpl struct {
	writeDatabaseTemplate    DatabaseTemplate
	readDatabaseTemplateList []DatabaseTemplate
	nextReaderIdx            int
}

func NewRWDatabaseTemplate(writeDatabaseTemplate DatabaseTemplate, readDatabaseTemplateList []DatabaseTemplate) DatabaseTemplate {
	impl := &DatabaseTemplateRWImpl{
		writeDatabaseTemplate:    writeDatabaseTemplate,
		readDatabaseTemplateList: readDatabaseTemplateList}
	return impl
}

func (this *DatabaseTemplateRWImpl) IsSharding() bool {
	return false
}
func (this *DatabaseTemplateRWImpl) GetShardingCount() int {
	return 1
}

func (this *DatabaseTemplateRWImpl) GetDatabaseTemplateShardingBySum(s key.Sum) (DatabaseTemplate, int, error) {
	return this, 0, nil
}
func (this *DatabaseTemplateRWImpl) GetDatabaseTemplateByShardingIdx(idx int) (DatabaseTemplate, error) {
	return this, nil
}

func (this *DatabaseTemplateRWImpl) GetReadDatabaseTemplate() DatabaseTemplate {
	if len(this.readDatabaseTemplateList) == 0 {
		return this.writeDatabaseTemplate
	}

	idx := this.nextReaderIdx % len(this.readDatabaseTemplateList)
	return this.readDatabaseTemplateList[idx]
}

func (this *DatabaseTemplateRWImpl) GetWriteDatabaseTemplate() DatabaseTemplate {
	return this.writeDatabaseTemplate
}

func (this *DatabaseTemplateRWImpl) Close() (err error) {
	if this.writeDatabaseTemplate == nil {
		return nil
	}
	err = this.writeDatabaseTemplate.Close()
	this.writeDatabaseTemplate = nil
	for idx, _ := range this.readDatabaseTemplateList {
		e := this.readDatabaseTemplateList[idx].Close()
		if e != nil {
			err = e
		}

	}
	this.readDatabaseTemplateList = nil
	return
}

func (this *DatabaseTemplateRWImpl) QueryArray(sum key.Sum, sql string, mapRow MapRow, params ...interface{}) ([]interface{}, error) {
	return this.GetReadDatabaseTemplate().QueryArray(sum, sql, mapRow, params...)
}

func (this *DatabaseTemplateRWImpl) QueryObject(sum key.Sum, sql string, mapRow MapRow, params ...interface{}) (interface{}, error) {
	return this.GetReadDatabaseTemplate().QueryObject(sum, sql, mapRow, params...)
}

func (this *DatabaseTemplateRWImpl) Exec(sum key.Sum, sql string, params ...interface{}) (err error) {
	return this.GetWriteDatabaseTemplate().Exec(sum, sql, params...)
}

func (this *DatabaseTemplateRWImpl) ExecDDL(sql string, params ...interface{}) (err error) {
	return this.GetWriteDatabaseTemplate().ExecDDL(sql, params...)
}

func (this *DatabaseTemplateRWImpl) ExecForResult(sum key.Sum, sql string, params ...interface{}) (sql.Result, error) {
	return this.GetWriteDatabaseTemplate().ExecForResult(sum, sql, params...)
}
