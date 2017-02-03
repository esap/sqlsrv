// odbc查询工具函数 By woylin 2016.6.14
package sqlsrv

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"

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
	if db != nil {
		return nil
	}
	if dc != nil {
		return nil
	}
	c, err := ioutil.ReadFile(conf)
	if err != nil {
		log.Println("读SQL配置文件出错:", err)
		return err
	}

	if err := json.Unmarshal(c, &dc); err != nil {
		log.Println("解析SQL配置文件出错:", err)
		return err
	}
	linkDb()
	return nil
}

func linkDb() {
	//	dsn := fmt.Sprintf("server=%s;User id=%s;password=%s;database=%s",
	dsn := fmt.Sprintf("driver={SQL Server};SERVER=%s;UID=%s;PWD=%s;DATABASE=%s",
		dc.Server, dc.UserId, dc.Pwd, dc.DbName)
	//	db1, err := sql.Open("mssql", dsn)
	db1, err := sql.Open("odbc", dsn)
	checkErr(err)
	db = db1
}

// ChangeDb 更改配置
func ChangeDb(s ...string) {
	if len(s) == 4 {
		dc = &DbConf{s[0], s[1], s[2], s[3]}
		linkDb()
	}
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
	checkErr(err)
	if !rs.Next() {
		return false
	}
	return true
}

// FetchOne 返回单行
func FetchOne(query string, cond ...interface{}) *[]interface{} {
	if checkDB() != nil {
		return nil
	}
	row := db.QueryRow(query, cond...)
	result := make([]interface{}, 0)
	onerow := make([]interface{}, 1)
	err := row.Scan(onerow...)
	if err != nil {
		panic(err)
	}
	result = append(result, onerow)
	return &result
}

// FetchAll 返回所有行
func FetchAll(query string, cond ...interface{}) *[]interface{} {
	if checkDB() != nil {
		return nil
	}
	rows, err := db.Query(query, cond...)
	if err != nil {
		log.Println("FetchAll.Query:", err)
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
	return &result
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
		err = rows.Scan(scanArgs...)
		if err != nil {
			continue
		}
		data := make(map[string]interface{}) //数据行，含字段名
		for i, _ := range onerow {
			data[cols[i]] = conv(onerow[i])
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
		log.Println("sqlsrv.fetch()->", err)
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
	checkErr(err)
	defer rows.Close()
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
	if checkDB() != nil {
		return &result
	}
	rows, err := db.Query(query, cond...)
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
func FetchOnePtr(query string, struc interface{}, cond ...interface{}) *interface{} {
	if checkDB() != nil {
		return nil
	}
	rows, err := db.Query(query, cond...)
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
