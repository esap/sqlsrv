package sqlsrv

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
)

// Db 数据库连接
type Db struct {
	*sql.DB
}

func NewDb(conf *DbConf) (d *Db, err error) {
	d = new(Db)
	dsn := fmt.Sprintf("driver={SQL Server};SERVER=%s;UID=%s;PWD=%s;DATABASE=%s",
		conf.Server, conf.UserId, conf.Pwd, conf.DbName)
	db, err := sql.Open("odbc", dsn)
	return &Db{db}, err
}

// CheckBool 检查是否存在
func (d *Db) CheckBool(sql string, cond ...interface{}) bool {
	rs, err := d.Query(sql, cond...)
	defer rs.Close()
	checkErr(err)
	return rs.Next()
}

// FetchAll 返回所有行
func (d *Db) FetchAll(query string, cond ...interface{}) (*[]interface{}, error) {
	rows, err := d.Query(query, cond...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	checkErr(err)
	leng := len(cols)
	result := make([]interface{}, 0)      // 结果集，数组
	scanArgs := make([]interface{}, leng) // 扫描专用指针
	onerow := make([]interface{}, leng)   // 数据行，无字段名
	for i := range onerow {
		scanArgs[i] = &onerow[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			continue
		}
		data := make(map[string]interface{}) // 数据行，含字段名
		for i, _ := range onerow {
			data[cols[i]] = conv(onerow[i])
		}
		result = append(result, data)
	}
	return &result, nil
}

// Fetch 返回单值
func (d *Db) Fetch(query string, cond ...interface{}) (interface{}, error) {
	var rst interface{}
	err := d.QueryRow(query, cond...).Scan(&rst)
	if err != nil {
		return nil, err
	}
	rst = conv(rst)
	return &rst, nil
}

// NumRows 返回行数
func (d *Db) NumRows(query string, cond ...interface{}) int {
	rows, err := d.Query(query, cond...)
	checkErr(err)
	defer rows.Close()
	result := 0
	for rows.Next() {
		result++
	}
	return result
}

// Exec 执行SQL，update/insert/delete
func (d *Db) Exec(query string, cond ...interface{}) error {
	stmt, err := d.Prepare(query)
	checkErr(err)
	defer stmt.Close()
	_, err = stmt.Exec(cond...)
	if err != nil {
		return err
	}
	return nil
}

// FetchAllRowsPtr 通用查询
func (d *Db) FetchAllRowsPtr(query string, struc interface{}, cond ...interface{}) *[]interface{} {
	result := make([]interface{}, 0)
	rows, err := d.Query(query, cond...)
	if err != nil {
		return &result
	}
	defer rows.Close()
	s := reflect.ValueOf(struc).Elem()
	leng := s.NumField()
	onerow := make([]interface{}, leng)
	for i := 0; i < leng; i++ {
		onerow[i] = s.Field(i).Addr().Interface()
	}
	for rows.Next() {
		err = rows.Scan(onerow...)
		if err != nil {
			return &result
		}
		result = append(result, s.Interface())
	}
	return &result
}

// FetchOnePtr 通用查询单条
func (d *Db) FetchOnePtr(query string, struc interface{}, cond ...interface{}) *interface{} {
	rows, err := d.Query(query, cond...)
	if err != nil {
		fmt.Println("FetchOnePtr>>>", err)
		return nil
	}
	defer rows.Close()
	s := reflect.ValueOf(struc).Elem()
	leng := s.NumField()
	onerow := make([]interface{}, leng)
	for i := 0; i < leng; i++ {
		onerow[i] = s.Field(i).Addr().Interface()
	}
	if rows.Next() {
		err = rows.Scan(onerow...)
		if err != nil {
			log.Println(err)
			return nil
		}
	}
	result := s.Interface()
	return &result
}
