package sqlbp

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// whereItem 用于生成where条件的辅助结构体
type whereItem struct {
	field string
	op    string
	value interface{}
}

// dataItem 用于生成update语句的辅助结构体
type dataItem struct {
	field string
	op    string
	value interface{}
}

type queryInfo struct {
	// 查询字段，默认是 *
	selectField []string

	// 表的别名
	as string

	// 联表语句，example: left join user as u on table.uid = u.id
	join string

	// 绑定参数列表
	whereParams []interface{}

	// 用于生成查询条件SQL，详情请参数getWhereSql的备注
	// 当该字段非空时，whereRaw，WhereParams这两个字段将会失效
	where []whereItem

	//取默认 是dao.GetTableName() 也可以自己赋值
	tableName string

	// having查询语句
	having string

	// 排序字段
	order string

	// 分组字段
	group string

	// 开始查询的offset
	offset int64

	// 结果集数量，默认是1024
	limit int64

	// 页数，该字段大于0时，会和limit一起生成 limit XX,XX语句，当它存在时Offset不起作用
	page int64

	// 强制使用主库查询
	queryUseMaster bool
}

func createWhereItem(field string, op string, value interface{}) whereItem {
	return whereItem{field: field, op: op, value: value}
}

// selectByWrapper 执行多条数据的查询请求
// note: 查询结果不支持null，如果数据库中有null，请使用NullToEmpty或NullToZero转换一下
func selectByWrapper(
	ctx context.Context,
	dest interface{},
	dao *BaseDao,
	w *Wrapper,
) (err error) {
	connect, err := getConnectByWrapper(ctx, dao, w, false)
	if err != nil {
		return
	}

	sql, params, err := getSelectSql(dao.GetTableName(), w.queryInfo)
	if err != nil {
		return
	}

	err = connect.SelectContext(ctx, dest, sql, params...)
	if err != nil {
		return
	}
	return
}

// SelectMapByWrapper 执行多条数据的查询请求，生成[]map[string]interface{}
// 如果表中有blob类型的字段，请不要使用该函数，会产生乱码
func selectMapByWrapper(
	ctx context.Context,
	dao *BaseDao,
	w *Wrapper,
) (result []map[string]interface{}, err error) {
	connect, err := getConnectByWrapper(ctx, dao, w, false)
	if err != nil {
		return
	}

	sql, params, err := getSelectSql(dao.GetTableName(), w.queryInfo)
	if err != nil {
		return
	}

	rows, err := connect.QueryContext(ctx, sql, params...)
	if err != nil {
		return
	}

	columns, err := rows.Columns()
	if err != nil {
		return
	}
	defer rows.Close()

	// 临时存储每行数据，并为每一列初始化一个指针
	columnLength := len(columns)
	buffer := make([]interface{}, columnLength)
	for index, _ := range buffer {
		var current interface{}
		buffer[index] = &current
	}

	for rows.Next() {
		err = rows.Scan(buffer...)
		if err != nil {
			return
		}
		item := make(map[string]interface{})
		for i, data := range buffer {
			// json.Marshal会将[]byte数组格式化成base64，这里统一转化成string
			v := *data.(*interface{})
			b, ok := v.([]byte)
			if ok {
				item[columns[i]] = string(b)
			} else {
				item[columns[i]] = v
			}
		}
		result = append(result, item)
	}

	return
}

// getOneData 通过ID查询单条记录（ID可以是表中的任何字段）
func getOneData(
	ctx context.Context,
	dao *BaseDao,
	dest interface{},
	idKey string,
	id interface{},
) (err error) {
	dbName := dao.GetSlaveDbName()
	connect, err := getDbConnect(dbName)
	if err != nil {
		return
	}

	var query queryInfo
	query.where = []whereItem{createWhereItem(idKey, "=", id)}
	sql, params, err := getSelectOneSql(dao, query)
	if err != nil {
		return
	}

	err = connect.GetContext(ctx, dest, sql, params...)
	if err != nil {
		return
	}
	return
}

// countByWrapper 查询条数
func countByWrapper(
	ctx context.Context,
	dao *BaseDao,
	w *Wrapper,
) (result int64, err error) {
	connect, err := getConnectByWrapper(ctx, dao, w, false)
	if err != nil {
		return
	}

	// 注意，w是作为输入参数使用的，不能直接改w.queryInfo，
	query := w.queryInfo
	query.selectField = []string{"count(1) as cn"}
	sql, params, err := getSelectOneSql(dao, query)
	if err != nil {
		return
	}

	err = connect.GetContext(ctx, &result, sql, params...)
	if err != nil {
		return
	}
	return
}

// insertByWrapper 插入数据
func insertByWrapper(
	ctx context.Context,
	dao *BaseDao,
	w *Wrapper,
) (lastId int64, err error) {
	connect, err := getConnectByWrapper(ctx, dao, w, true)
	if err != nil {
		return
	}

	params := make([]interface{}, 0)
	sql, err := getInsertSql(dao, w.dataItems, &params)
	if err != nil {
		return
	}

	ret, err := connect.ExecContext(ctx, sql, params...)
	if err != nil {
		return
	}

	lastId, err = ret.LastInsertId()
	if err != nil {
		return
	}

	return
}

// deleteByWrapper 删除数据
func deleteByWrapper(
	ctx context.Context,
	dao *BaseDao,
	w *Wrapper,
) (affectedRow int64, err error) {
	connect, err := getConnectByWrapper(ctx, dao, w, true)
	if err != nil {
		return
	}

	params := make([]interface{}, 0)
	sql, err := getDeleteSql(dao, w.queryInfo.where, &params)
	if err != nil {
		return
	}

	ret, err := connect.ExecContext(ctx, sql, params...)
	if err != nil {
		return
	}

	affectedRow, err = ret.RowsAffected()
	if err != nil {
		return
	}

	return
}

// updateByWrapper 更新数据
func updateByWrapper(
	ctx context.Context,
	dao *BaseDao,
	w *Wrapper,
) (affectedRow int64, err error) {
	connect, err := getConnectByWrapper(ctx, dao, w, true)
	if err != nil {
		return
	}

	params := make([]interface{}, 0)
	sql, err := getUpdateSql(dao, w.dataItems, w.queryInfo.where, &params)
	if err != nil {
		return
	}

	ret, err := connect.ExecContext(ctx, sql, params...)
	if err != nil {
		return
	}

	affectedRow, err = ret.RowsAffected()
	if err != nil {
		return
	}

	return
}

// 生成Where语句
// where 查询条件的数组 example:
//
//	where = []whereItem {
//	    {"one", "in", []int{1, 2, 3}}, // ("in", "not in")
//	    {"two", "between", []interface{}{10, 20}}, // ("between", "not between")
//	    {"three", ">", 10}, // ("=", "!=", "<", "<=", ">", ">=", "<>", "like")
//	    {"four", "apply", []interface{}{"a = ? or b > ? and c > ?", 1, 2, 3}}, // ("apply")
//	}
func getWhereSql(
	where []whereItem,
	params *[]interface{},
) (result string, err error) {
	if len(where) == 0 {
		result = ""
		return
	}

	whereList := make([]string, 0)
	for _, item := range where {
		field := keyFormat(item.field)
		value := item.value
		kind := reflect.ValueOf(value).Kind()

		if kind == reflect.Map {
			err = fmt.Errorf("where value type is not allow map: %v", item)
			return
		}

		if item.op == "" {
			err = fmt.Errorf("op is not allow empty: %v", item)
			return
		}

		var current string
		if item.op == "=" || item.op == "!=" || item.op == "<=" || item.op == "<" || item.op == ">=" ||
			item.op == ">" || item.op == "<>" || item.op == "like" {
			whereList = append(whereList, fmt.Sprintf("%s %s ?", field, item.op))
			*params = append(*params, item.value)
		} else if item.op == "in" || item.op == "not in" {
			current, err = getWhereInSql(field, item.op, item.value, params)
			whereList = append(whereList, current)
		} else if item.op == "between" || item.op == "not between" {
			current, err = getWhereBetweenSql(field, item.op, item.value, params)
			whereList = append(whereList, current)
		} else if item.op == "apply" {
			current, err = getWhereApplySql(field, item.value, params)
			whereList = append(whereList, current)
		} else {
			err = fmt.Errorf("db operate %s is not support, [field:%s]", item.op, field)
		}

		if err != nil {
			return
		}
	}

	result = strings.Join(whereList, fmt.Sprintf(" %s ", "and"))
	return
}

// 生成between的where语句
func getWhereBetweenSql(key string, op string, value interface{}, params *[]interface{}) (result string, err error) {
	if reflect.ValueOf(value).Kind() != reflect.Slice {
		err = fmt.Errorf("[%s] where between format error", key)
		return
	}

	list, err := interfaceToSlice(value)
	if err != nil {
		return
	}
	if len(list) != 2 {
		err = fmt.Errorf("[%s] params between count error", key)
		return
	}

	result = fmt.Sprintf("%s %s ? and ?", key, op)
	*params = append(*params, list[0], list[1])
	return
}

// 生成in的where语句
func getWhereInSql(key string, op string, value interface{}, params *[]interface{}) (result string, err error) {
	if reflect.ValueOf(value).Kind() != reflect.Slice {
		err = fmt.Errorf("[%s] where in is not an array", key)
		return
	}

	list, err := interfaceToSlice(value)
	if err != nil {
		return
	}
	if len(list) == 0 {
		err = fmt.Errorf("[%s] where in is not allow empty", key)
		return
	}

	for _, v := range list {
		result += "?, "
		*params = append(*params, v)
	}
	result = result[:len(result)-2]
	result = fmt.Sprintf("%s %s (%s)", key, op, result)
	return
}

// 生成between的where语句
func getWhereApplySql(key string, value interface{}, params *[]interface{}) (result string, err error) {
	if reflect.ValueOf(value).Kind() != reflect.Slice {
		err = fmt.Errorf("[%s] where params format error", key)
		return
	}

	list, err := interfaceToSlice(value)
	if err != nil {
		return
	}
	if len(list) < 1 {
		err = fmt.Errorf("[%s] where params count error", key)
		return
	}

	if reflect.ValueOf(list[0]).Kind() != reflect.String {
		err = fmt.Errorf("[%s] where params kind error", key)
		return
	}

	result = fmt.Sprintf("(%s)", list[0])
	*params = append(*params, list[1:]...)
	return
}

// 生成 delete sql
func getDeleteSql(
	dao *BaseDao,
	where []whereItem,
	params *[]interface{},
) (result string, err error) {
	// 不支持全表删除的操作，容易出事故
	if len(where) == 0 {
		err = fmt.Errorf("not support delete all records from tableName")
		return
	}

	wherePart, err := getWhereSql(where, params)
	if err != nil {
		return
	}
	result = fmt.Sprintf("delete from %s where %s", dao.GetTableName(), wherePart)

	return
}

// 生成update sql
func getUpdateSql(
	dao *BaseDao,
	data []dataItem,
	where []whereItem,
	params *[]interface{},
) (result string, err error) {
	if len(data) == 0 {
		err = fmt.Errorf("insert data is not allow empty")
		return
	}

	// 不支持全表更新的操作，容易出事故
	if len(where) == 0 {
		err = fmt.Errorf("not support update all records from tableName")
		return
	}

	tableName := dao.GetTableName()
	var dataPart, wherePart string

	for _, item := range data {
		if item.op == "value" {
			dataPart += fmt.Sprintf("`%s` = ?, ", item.field)
			*params = append(*params, item.value)
		} else if item.op == "exp" {
			dataPart += fmt.Sprintf("`%s` = %s, ", item.field, item.value)
		}
	}
	dataPart = dataPart[0 : len(dataPart)-2]

	wherePart, err = getWhereSql(where, params)
	if err != nil {
		return
	}

	result = fmt.Sprintf("update %s set %s where %s", tableName, dataPart, wherePart)
	return
}

// 生成select sql
func getSelectSql(tableName string, info queryInfo) (result string, params []interface{}, err error) {
	if info.tableName != "" {
		tableName = info.tableName
	}
	if tableName == "" {
		err = fmt.Errorf("table name is empty")
		return
	}
	selectPart := "*"
	var wherePart, havingPart, groupPart, orderPart, limitPart, joinPart, asPart string

	if len(info.selectField) != 0 {
		selectPart = strings.Join(info.selectField, ",")
	}
	if info.as != "" {
		asPart = " " + info.as
	}
	if info.join != "" {
		joinPart = " " + info.join
	}
	if len(info.where) != 0 {
		wherePart, err = getWhereSql(info.where, &info.whereParams)
		if err != nil {
			return
		}
		wherePart = " where " + wherePart
	}
	if info.having != "" {
		havingPart = " having " + havingPart
	}
	if info.order != "" {
		orderPart = " order by " + info.order
	}
	if info.group != "" {
		groupPart = " group by " + info.group
	}

	if info.limit == 0 {
		info.limit = 1024
	}
	start := info.offset
	if info.page > 1 {
		start = (info.page - 1) * info.limit
	}
	if start == 0 {
		limitPart = fmt.Sprintf(" limit %d", info.limit)
	} else {
		limitPart = fmt.Sprintf(" limit %d, %d", start, info.limit)
	}

	params = info.whereParams
	result = fmt.Sprintf(
		"select %s from %s%s%s%s%s%s%s%s",
		selectPart,
		tableName,
		asPart,
		joinPart,
		wherePart,
		groupPart,
		havingPart,
		orderPart,
		limitPart,
	)

	return
}

// 生成select sql（单条）
func getSelectOneSql(
	dao *BaseDao,
	info queryInfo,
) (result string, params []interface{}, err error) {
	tableName := dao.GetTableName()
	var wherePart, orderPart, joinPart, selectPart string

	selectPart = "*"
	if len(info.selectField) != 0 {
		selectPart = strings.Join(info.selectField, ",")
	}
	if len(info.where) != 0 {
		wherePart, err = getWhereSql(info.where, &info.whereParams)
		if err != nil {
			return
		}
		wherePart = " where " + wherePart
	}
	if len(info.order) != 0 {
		orderPart = " order by " + info.order
	}
	if len(info.join) != 0 {
		joinPart = " " + info.join
	}

	params = info.whereParams
	result = fmt.Sprintf(
		"select %s from %s%s%s%s limit 0, 1",
		selectPart,
		tableName,
		joinPart,
		wherePart,
		orderPart,
	)

	return
}

// 生成insert sql
func getInsertSql(
	dao *BaseDao,
	data []dataItem,
	params *[]interface{},
) (result string, err error) {
	if len(data) == 0 {
		err = fmt.Errorf("insert data is not allow empty")
		return
	}

	var fieldString string
	var valueString string

	for _, item := range data {
		fieldString += fmt.Sprintf("`%s`, ", item.field)
		valueString += "?, "
		*params = append(*params, item.value)
	}
	fieldString = fieldString[:len(fieldString)-2]
	valueString = valueString[:len(valueString)-2]

	result = fmt.Sprintf("insert into `%s`(%s) values (%s)", dao.GetTableName(), fieldString, valueString)

	return
}

func getConnectByWrapper(
	ctx context.Context,
	dao *BaseDao,
	w *Wrapper,
	useMaster bool,
) (
	conn connectInter,
	err error,
) {
	tx := GetCtxTransaction(ctx)
	if tx != nil {
		conn = tx
		return
	}

	var name string
	if useMaster || (w != nil && w.queryInfo.queryUseMaster) {
		name = dao.GetDbName()
	} else {
		name = dao.GetSlaveDbName()
	}

	conn, err = getDbConnect(name)
	if err != nil {
		return
	}
	return
}
