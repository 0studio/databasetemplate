package databasetemplate

import (
	"database/sql"
	"errors"
	key "github.com/0studio/storage_key"
)

func NewDatabaseTemplateSharding(dbList []*sql.DB) DatabaseTemplate {
	var dtList []DatabaseTemplate = make([]DatabaseTemplate, len(dbList))
	for idx, _ := range dbList {
		dtList[idx] = &DatabaseTemplateImpl{dbList[idx]}
	}
	return &DatabaseTemplateImplShardingImpl{dtList}
}

type DatabaseTemplateImplShardingImpl struct {
	dtList []DatabaseTemplate
}

// func (this *DatabaseTemplateImplShardingImpl) GetConn() *sql.DB {
// 	return this.Conn
// }

// func (this *DatabaseTemplateImplShardingImpl) IsConnOk() (ok bool) {
// 	if this.Conn == nil {
// 		return false
// 	}
// 	return this.Conn.Ping() == nil
// }
func (this *DatabaseTemplateImplShardingImpl) Close() (err error) {
	for _, dt := range this.dtList {
		e := dt.Close()
		if e != nil {
			err = e
		}
	}
	return
}

func (this *DatabaseTemplateImplShardingImpl) GetReadDatabaseTemplate() DatabaseTemplate {
	return this
}
func (this *DatabaseTemplateImplShardingImpl) GetWriteDatabaseTemplate() DatabaseTemplate {
	return this
}

func (this *DatabaseTemplateImplShardingImpl) IsSharding() bool {
	return true
}
func (this *DatabaseTemplateImplShardingImpl) GetShardingCount() int {
	return len(this.dtList)
}

func (this *DatabaseTemplateImplShardingImpl) GetDatabaseTemplateByShardingIdx(idx int) (DatabaseTemplate, error) {
	if idx >= len(this.dtList) {
		return nil, errors.New("datatemplate_idx_overflow")
	}

	return this.dtList[idx], nil
}
func (this *DatabaseTemplateImplShardingImpl) GetDatabaseTemplateShardingBySum(s key.Sum) (DatabaseTemplate, int, error) {
	if len(this.dtList) == 0 {
		return nil, 0, errors.New("empty_datatemplate_list")
	}

	idx := s.ToSum() % len(this.dtList)
	return this.dtList[idx], idx, nil
}

type resultChan struct {
	result []interface{}
	err    error
}

func (this *DatabaseTemplateImplShardingImpl) QueryArray(sum key.Sum, sql string, mapRow MapRow, params ...interface{}) (list []interface{}, err error) {
	var dt DatabaseTemplate
	if sum == nil { // 占不支持从所有库查询
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
		dt, _, err = this.GetDatabaseTemplateShardingBySum(sum)
		if err != nil {
			return
		}
		list, err = dt.QueryArray(sum, sql, mapRow, params...)
		return
	}
	var dtIdxMap map[int]DatabaseTemplate = make(map[int]DatabaseTemplate)
	var dtIdx int
	for idx := 0; idx < sum.SumLen(); idx++ {
		subSum := sum.GetSumByIdx(idx)
		dt, dtIdx, err = this.GetDatabaseTemplateShardingBySum(subSum)
		if err != nil {
			return
		}
		dtIdxMap[dtIdx] = dt
	}
	if len(dtIdxMap) == 1 {
		for _, dt := range dtIdxMap {
			tmpList, e := dt.QueryArray(nil, sql, mapRow, params...)
			return tmpList, e
		}
	} else {
		resultsChannel := make(chan resultChan, len(dtIdxMap))
		for _, dt := range dtIdxMap {
			go func() {
				tmpList, e := dt.QueryArray(nil, sql, mapRow, params...)
				// if e != nil {
				// 	err = e
				// 	continue
				// }
				resultsChannel <- resultChan{tmpList, e}
			}()
		}
		for _, _ = range dtIdxMap {
			result := <-resultsChannel
			if result.err != nil {
				err = result.err
			} else {
				list = append(list, result.result...)
			}
		}
	}

	return
}

// func (this *DatabaseTemplateImplShardingImpl) QueryIntoArray(resultList interface{}, sql string, mapRow MapRow, params ...interface{}) error {

// }

func (this *DatabaseTemplateImplShardingImpl) QueryObject(sum key.Sum, sql string, mapRow MapRow, params ...interface{}) (obj interface{}, err error) {
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
		dt, _, err = this.GetDatabaseTemplateShardingBySum(sum)
		if err != nil {
			return
		}
		obj, err = dt.QueryObject(sum, sql, mapRow, params...)
		return
	}
	return
}

func (this *DatabaseTemplateImplShardingImpl) Exec(sum key.Sum, sql string, params ...interface{}) (err error) {
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
		dt, _, err = this.GetDatabaseTemplateShardingBySum(sum)
		if err != nil {
			return
		}
		e := dt.Exec(sum, sql, params...)
		if e != nil {
			err = e
		}
		return
	}

	var dtIdxMap map[int]DatabaseTemplate = make(map[int]DatabaseTemplate)
	var dtIdx int
	for idx := 0; idx < sum.SumLen(); idx++ {
		subSum := sum.GetSumByIdx(idx)
		dt, dtIdx, err = this.GetDatabaseTemplateShardingBySum(subSum)
		if err != nil {
			return
		}
		dtIdxMap[dtIdx] = dt
	}
	for _, dt := range dtIdxMap {
		e := dt.Exec(nil, sql, params...)
		if e != nil {
			err = e
		}
	}
	return
}

func (this *DatabaseTemplateImplShardingImpl) ExecForResult(sum key.Sum, sql string, params ...interface{}) (result sql.Result, err error) {
	var dt DatabaseTemplate
	if sum.SumLen() == 1 {
		dt, _, err = this.GetDatabaseTemplateShardingBySum(sum)
		if err != nil {
			return
		}
		result, err = dt.ExecForResult(sum, sql, params...)
		return
	}
	return
}
func (this *DatabaseTemplateImplShardingImpl) ExecDDL(sql string, params ...interface{}) (err error) {
	var dt DatabaseTemplate
	for _, dt = range this.dtList {
		e := dt.ExecDDL(sql, params...)
		if e != nil {
			err = e
		}
	}
	return

}
