package sqlsrv

import (
	"time"
)

// 转换sqlserver值格式
func conv(pval interface{}) interface{} {
	switch v := (pval).(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	default:
		return v
	}
}

// 错误检查
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
