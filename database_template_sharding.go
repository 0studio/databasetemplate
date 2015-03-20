package databasetemplate

import (
	"database/sql"
	"errors"
	key "github.com/0studio/storage_key"
)

func NewDatabaseTemplateSplit(dbList []*sql.DB) DatabaseTemplate {
	var dtList []DatabaseTemplate = make([]DatabaseTemplate, len(dbList))
	for idx, _ := range dbList {
		dtList[idx] = &DatabaseTemplateImpl{dbList[idx]}
	}
	return &DatabaseTemplateImplSplitImpl{dtList}
}

type DatabaseTemplateImplSplitImpl struct {
	dtList []DatabaseTemplate
}

// func (this *DatabaseTemplateImplSplitImpl) GetConn() *sql.DB {
// 	return this.Conn
// }

// func (this *DatabaseTemplateImplSplitImpl) IsConnOk() (ok bool) {
// 	if this.Conn == nil {
// 		return false
// 	}
// 	return this.Conn.Ping() == nil
// }
func (this *DatabaseTemplateImplSplitImpl) Close() (err error) {
	for _, dt := range this.dtList {
		e := dt.Close()
		if e != nil {
			err = e
		}
	}
	return
}
func (this *DatabaseTemplateImplSplitImpl) GetDatabaseTemplateBySum(s key.Sum) (DatabaseTemplate, error) {
	if len(this.dtList) == 0 {
		return nil, errors.New("empty_datatemplate_list")
	}

	idx := s.ToSum() % len(this.dtList)
	return this.dtList[idx], nil
}
func (this *DatabaseTemplateImplSplitImpl) QueryArray(sum key.Sum, sql string, mapRow MapRow, params ...interface{}) (list []interface{}, err error) {
	var dt DatabaseTemplate
	if sum == nil { // 从所有库查询
		for _, dt = range this.dtList {
			tmpList, e := dt.QueryArray(sum, sql, mapRow, params...)
			if e != nil {
				err = e
				continue
			}
			list = append(list, tmpList...)
		}
		return
	}
	if sum.SumLen() == 1 {
		dt, err = this.GetDatabaseTemplateBySum(sum)
		if err != nil {
			return
		}
		list, err = dt.QueryArray(sum, sql, mapRow, params...)
		return
	}
	for idx := 0; idx < sum.SumLen(); idx++ {
		subSum := sum.GetSumByIdx(idx)
		dt, err = this.GetDatabaseTemplateBySum(subSum)
		if err != nil {
			return
		}
		tmpList, e := dt.QueryArray(sum, sql, mapRow, params...)
		if e != nil {
			err = e
			continue
		}
		list = append(list, tmpList...)
	}
	return
}

// func (this *DatabaseTemplateImplSplitImpl) QueryIntoArray(resultList interface{}, sql string, mapRow MapRow, params ...interface{}) error {

// }

func (this *DatabaseTemplateImplSplitImpl) QueryObject(sum key.Sum, sql string, mapRow MapRow, params ...interface{}) (obj interface{}, err error) {
	var dt DatabaseTemplate
	if sum == nil { // 从所有库查询
		for _, dt = range this.dtList {
			obj, err = dt.QueryObject(sum, sql, mapRow, params...)
			if err == nil && obj != nil {
				return
			}
		}
		return
	}

	if sum.SumLen() == 1 {
		dt, err = this.GetDatabaseTemplateBySum(sum)
		if err != nil {
			return
		}
		obj, err = dt.QueryObject(sum, sql, mapRow, params...)
		return
	}
	return
}

func (this *DatabaseTemplateImplSplitImpl) Exec(sum key.Sum, sql string, params ...interface{}) (err error) {
	var dt DatabaseTemplate
	if sum == nil { // 从所有库查询
		for _, dt = range this.dtList {
			e := dt.Exec(sum, sql, params...)
			if e != nil {
				err = e
			}
		}
		return
	}

	if sum.SumLen() == 1 {
		dt, err = this.GetDatabaseTemplateBySum(sum)
		if err != nil {
			return
		}
		e := dt.Exec(sum, sql, params...)
		if e != nil {
			err = e
		}
		return
	}

	for idx := 0; idx < sum.SumLen(); idx++ {
		subSum := sum.GetSumByIdx(idx)
		dt, err = this.GetDatabaseTemplateBySum(subSum)
		e := dt.Exec(subSum, sql, params...)
		if e != nil {
			err = e
		}
	}
	return
}

func (this *DatabaseTemplateImplSplitImpl) ExecForResult(sum key.Sum, sql string, params ...interface{}) (result sql.Result, err error) {
	var dt DatabaseTemplate
	if sum.SumLen() == 1 {
		dt, err = this.GetDatabaseTemplateBySum(sum)
		if err != nil {
			return
		}
		result, err = dt.ExecForResult(sum, sql, params...)
		return
	}
	return
}
func (this *DatabaseTemplateImplSplitImpl) ExecDDL(sql string, params ...interface{}) (err error) {
	var dt DatabaseTemplate
	for _, dt = range this.dtList {
		e := dt.ExecDDL(sql, params...)
		if e != nil {
			err = e
		}
	}
	return

}
