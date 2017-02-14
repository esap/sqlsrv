package sqlsrv

import (
	"fmt"
	"regexp"
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

// 转换sqlserver值格式
func conStr(pval interface{}) string {
	switch v := (pval).(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprint(v)
	}
}

// 错误检查
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// SqlChk 防sql注入
func SqlChk(s string) string {
	str := `(?:')|(?:--)|(/\\*(?:.|[\\n\\r])*?\\*/)|(\b(select|update|and|or|delete|insert|trancate|char|chr|into|substr|ascii|declare|exec|count|master|into|drop|execute)\b)`
	re, err := regexp.Compile(str)
	if err != nil {
		return err.Error()
	}
	if re.MatchString(s) {
		return "valid sqlStr"
	}
	return s
}
