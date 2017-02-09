// odbc查询工具函数 By woylin 2016.6.14
package sqlsrv

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"strings"

	_ "github.com/alexbrainman/odbc"
	//	_ "github.com/denisenkom/go-mssqldb"
)

var (
	db   *sql.DB
	dc   *DbConf
	conf = "conf/db.json"
)

/*
配置文件位于conf/db.json,类似下列这样:
	{
		"UserId" :"sa",
		"Pwd"	 :"password",
		"Server" :"serverIP",
		"DbName" :"DBname"
	}
*/
type DbConf struct {
	UserId string
	Pwd    string
	Server string
	DbName string
}

// checkDB 检查DB是否连接，无则进行连接
func checkDB() error {
	if db != nil || dc != nil {
		return nil
	}
	c, err := ioutil.ReadFile(conf)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(c, &dc); err != nil {
		return err
	}
	return linkDb()
}

func linkDb() (err error) {
	//	dsn := fmt.Sprintf("server=%s;User id=%s;password=%s;database=%s",
	dsn := fmt.Sprintf("driver={SQL Server};SERVER=%s;UID=%s;PWD=%s;DATABASE=%s",
		dc.Server, dc.UserId, dc.Pwd, dc.DbName)
	//	db1, err := sql.Open("mssql", dsn)
	db, err = sql.Open("odbc", dsn)
	return err
}

// ChangeDb 更改配置
func ChangeDb(s ...string) error {
	if len(s) == 4 {
		dc = &DbConf{s[0], s[1], s[2], s[3]}
		return linkDb()
	}
	return errors.New("ChangeDb need 4 params")
}

// SetConf 配置DB参数
func SetConf(confPath string) {
	conf = confPath
}

// CheckBool 检查是否存在
func CheckBool(sql string, cond ...interface{}) bool {
	if checkDB() != nil {
		return false
	}
	rs, err := db.Query(sql, cond...)
	defer rs.Close()
	if err != nil {
		return false
	}
	return rs.Next()
}

// FetchOne 返回单行
func FetchOne(query string, cond ...interface{}) *map[string]interface{} {
	if checkDB() != nil {
		return nil
	}
	rows, err := db.Query(query, cond...)
	defer rows.Close()
	if err != nil {
		log.Println("FetchAll()->Query error:", err)
		return nil
	}
	cols, err := rows.Columns()
	checkErr(err)
	for k, v := range cols {
		cols[k] = strings.ToLower(v)
	}
	leng := len(cols)
	scanArgs := make([]interface{}, leng)      // 扫描专用指针
	onerow := make([]interface{}, leng)        // 数据行，无字段名
	data := make(map[string]interface{}, leng) // 数据行，含字段名
	for i := range onerow {
		scanArgs[i] = &onerow[i]
	}
	if rows.Next() {
		if rows.Scan(scanArgs...) != nil {
			return nil
		}
		for k, _ := range onerow {
			data[cols[k]] = conv(onerow[k])
		}
	}
	return &data
}

// FetchAll 返回所有行
func FetchAll(query string, cond ...interface{}) *[]interface{} {
	if checkDB() != nil {
		return nil
	}
	rows, err := db.Query(query, cond...)
	if err != nil {
		log.Println("FetchAll()->Query error:", err)
		return nil
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
		if rows.Scan(scanArgs...) != nil {
			continue
		}
		data := make(map[string]interface{}) // 数据行，含字段名
		for k, _ := range onerow {
			data[cols[k]] = conv(onerow[k])
		}
		result = append(result, data)
	}
	return &result
}

// FetchAllJson 返回所有行的Json
func FetchAllJson(sql string, cond ...interface{}) string {
	if checkDB() != nil {
		return ""
	}
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Println("FetchAllJson()->Prepare error:", err)
		return ""
	}
	defer stmt.Close()
	rows, err := stmt.Query(cond...)
	if err != nil {
		log.Println("FetchAllJson()->Query error:", err)
		return ""
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.Println("FetchAllJson()->Columns error:", err)
		return ""
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		log.Println("FetchAllJson()->Marshal error:", err)
		return ""
	}
	return string(jsonData)
}

type treeNode struct {
	Id       interface{}   `json:"id"`
	Text     interface{}   `json:"text"`
	Expanded bool          `json:"expanded"`
	Leaf     bool          `json:"leaf"`
	Children []interface{} `json:"children"`
}

func (t *treeNode) appendChild(c interface{}) {
	t.Children = append(t.Children, c)
}

// FetchMenuTree ES目录树
func FetchMenuTree(query string, cond ...interface{}) *treeNode {
	if checkDB() != nil {
		return nil
	}
	rows, err := db.Query(query, cond...)
	checkErr(err)
	defer rows.Close()
	cols, err := rows.Columns()
	checkErr(err)
	leng := len(cols)
	scanArgs := make([]interface{}, leng) //扫描专用指针
	onerow := make([]interface{}, leng)   //数据行，无字段名
	for i := range onerow {
		scanArgs[i] = &onerow[i]
	}
	treeMap := make(map[string]*treeNode, 0)
	tree := &treeNode{1, "root", true, false, make([]interface{}, 0)}
	for rows.Next() {
		if rows.Scan(scanArgs...) != nil {
			continue
		}
		data := make(map[string]interface{}) //数据行，含字段名
		for k, _ := range onerow {
			data[cols[k]] = conv(onerow[k])
		}
		menuName := fmt.Sprintf("%s", data["menu"])
		if _, ok := treeMap[menuName]; !ok {
			treeMap[menuName] = &treeNode{data["ordPath"], data["menu"], true, false, make([]interface{}, 0)}
			tree.appendChild(treeMap[menuName])
		}
		treeMap[menuName].appendChild(treeNode{data["id"], data["name"], false, true, nil})
		tree.appendChild(treeMap[menuName])

	}
	return tree
}

// Fetch 返回单值
func Fetch(query string, cond ...interface{}) *interface{} {
	if checkDB() != nil {
		return nil
	}
	var rst interface{}
	err := db.QueryRow(query, cond...).Scan(&rst)
	if err != nil {
		log.Println("Fetch()->QueryRow error:", err)
		return nil
	}
	rst = conv(rst)
	return &rst
}

// NumRows 返回行数
func NumRows(query string, cond ...interface{}) int {
	if checkDB() != nil {
		return 0
	}
	rows, err := db.Query(query, cond...)
	defer rows.Close()
	checkErr(err)
	result := 0
	for rows.Next() {
		result++
	}
	return result
}

// Exec 执行SQL，update/insert/delete
func Exec(query string, cond ...interface{}) error {
	stmt, err := db.Prepare(query)
	checkErr(err)
	defer stmt.Close()
	_, err = stmt.Exec(cond...)
	if err != nil {
		return err
	}
	return nil
}

// FetchAllRowsPtr 通用查询
func FetchAllRowsPtr(query string, struc interface{}, cond ...interface{}) *[]interface{} {
	result := make([]interface{}, 0)
	if err := checkDB(); err != nil {
		log.Println("FetchAllRowsPtr()->chechDB err", err)
		return &result
	}
	rows, err := db.Query(query, cond...)
	if err != nil {
		log.Println("FetchAllRowsPtr()->Query", err)
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
			log.Println("FetchAllRowsPtr()->Scan", err)
			return &result
		}
		result = append(result, s.Interface())
	}
	return &result
}

// FetchOnePtr 通用查询单条
func FetchOnePtr(query string, struc interface{}, cond ...interface{}) *interface{} {
	if checkDB() != nil {
		return nil
	}
	rows, err := db.Query(query, cond...)
	if err != nil {
		fmt.Println("FetchOnePtr()->FetchOnePtr error:", err)
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
