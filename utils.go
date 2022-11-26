package sqlbp

// 定义了一些辅助函数，仅供内部使用

import (
	"fmt"
	"github.com/fatih/structs"
	"reflect"
	"strings"
)

// structToDataItems 将结构体或map转为[]dataItem
func structToDataItems(data interface{}, tag string, ignoreKey string) ([]dataItem, error) {
	// 如果data是指针，就先处理一下
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	data = v.Interface()

	// 验证类型
	result := make([]dataItem, 0)
	v = reflect.ValueOf(data)

	if v.Kind() == reflect.Struct {
		structData := structs.New(data)
		for _, item := range structData.Fields() {
			tagValue := item.Tag(tag)
			value := item.Value()
			if tagValue == ignoreKey {
				continue
			}
			if tagValue != "" {
				curr := dataItem{field: tagValue, op: "value", value: value}
				result = append(result, curr)
			}
		}
	} else if v.Kind() == reflect.Map {
		dataMap := data.(map[string]interface{})
		for key, value := range dataMap {
			if key == ignoreKey {
				continue
			}
			curr := dataItem{field: key, op: "value", value: value}
			result = append(result, curr)
		}
	} else {
		return result, fmt.Errorf("structToDataItems error: data must struct or map, not allow %T", data)
	}
	return result, nil
}

// 将interface转化成interface slice
func interfaceToSlice(params interface{}) (result []interface{}, err error) {
	v := reflect.ValueOf(params)
	if v.Kind() != reflect.Slice {
		err = fmt.Errorf("interfaceToSlice: params is not slice")
		return
	}
	length := v.Len()
	result = make([]interface{}, length)
	for i := 0; i < length; i++ {
		result[i] = v.Index(i).Interface()
	}
	return
}

func keyFormat(key string) string {
	key = fmt.Sprintf("`%s`", key)
	key = strings.ReplaceAll(key, ".", "`.`")
	return key
}

// NullToDefaultString 框架不处理数据库值为null的情况，开发需求手动处理一下
func NullToDefaultString(name string, defaultValue string) string {
	return fmt.Sprintf("ifnull(%s, '%s') as %s", name, defaultValue, getFieldName(name))
}

// NullToDefaultNumber 框架不处理数据库值为null的情况，开发需求手动处理一下
func NullToDefaultNumber(name string, defaultValue int64) string {
	return fmt.Sprintf("ifnull(%s, %d) as %s", name, defaultValue, getFieldName(name))
}

func getFieldName(name string) string {
	pos := strings.Index(name, ".")
	if pos == -1 {
		return name
	} else {
		return name[pos+1:]
	}
}
