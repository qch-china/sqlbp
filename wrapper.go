package sqlbp

import (
	"encoding/json"
	"fmt"
	"github.com/jinzhu/copier"
)

/*
* SQL查询与更新的条件构造器
* 接口设计参考了Java Mybatis Plus条件构造器的设计来理念和接口名称，可参考：
* https://baomidou.com/pages/10c804
 */

type Wrapper struct {
	queryInfo queryInfo
	dataItems []dataItem
	errList   []error
}

func GetWrapper() *Wrapper {
	w := Wrapper{}
	w.dataItems = make([]dataItem, 0)
	w.queryInfo.where = make([]whereItem, 0)
	return &w
}

func CopyWrapper(src *Wrapper) (*Wrapper, error) {
	w := Wrapper{}
	err := copier.Copy(&w, src)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (w *Wrapper) GetError() error {
	if len(w.errList) == 0 {
		return nil
	}

	var msg string
	for _, err := range w.errList {
		msg += err.Error() + "; "
	}
	return fmt.Errorf("Wrapper error: %s", msg)
}

func (w *Wrapper) TableName(tableName string) *Wrapper {
	w.queryInfo.tableName = tableName
	return w
}

func (w *Wrapper) Limit(limit int64) *Wrapper {
	w.queryInfo.limit = limit
	return w
}

func (w *Wrapper) Page(page int64) *Wrapper {
	w.queryInfo.page = page
	return w
}

func (w *Wrapper) Offset(offset int64) *Wrapper {
	w.queryInfo.offset = offset
	return w
}

func (w *Wrapper) Group(group string) *Wrapper {
	w.queryInfo.group = group
	return w
}

func (w *Wrapper) Order(order string) *Wrapper {
	w.queryInfo.order = order
	return w
}

func (w *Wrapper) Having(having string) *Wrapper {
	w.queryInfo.having = having
	return w
}

func (w *Wrapper) Select(fields ...string) *Wrapper {
	if len(fields) == 0 {
		fields = []string{"*"}
	}
	w.queryInfo.selectField = fields
	return w
}

func (w *Wrapper) Eq(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, "=", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Ne(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, "!=", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Gt(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, ">", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Ge(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, ">=", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Lt(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, "<", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Le(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, "<=", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Between(column string, start interface{}, end interface{}) *Wrapper {
	item := createWhereItem(column, "between", []interface{}{start, end})
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) NotBetween(column string, start interface{}, end interface{}) *Wrapper {
	item := createWhereItem(column, "not between", []interface{}{start, end})
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Like(column string, value string) *Wrapper {
	item := createWhereItem(column, "like", fmt.Sprintf("%%%s%%", value))
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) NotLike(column string, value string) *Wrapper {
	item := createWhereItem(column, "not like", fmt.Sprintf("%%%s%%", value))
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) LikeLeft(column string, value string) *Wrapper {
	item := createWhereItem(column, "like", fmt.Sprintf("%%%s", value))
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) LikeRight(column string, value string) *Wrapper {
	item := createWhereItem(column, "like", fmt.Sprintf("%s%%", value))
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) In(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, "in", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) NotIn(column string, value interface{}) *Wrapper {
	item := createWhereItem(column, "not in", value)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Apply(params ...interface{}) *Wrapper {
	item := createWhereItem("", "apply", params)
	w.queryInfo.where = append(w.queryInfo.where, item)
	return w
}

func (w *Wrapper) Where(where []whereItem) *Wrapper {
	w.queryInfo.where = where
	return w
}

func (w *Wrapper) As(name string) *Wrapper {
	w.queryInfo.as = name
	return w
}

// Join 联表信息，这里就不作太深入的封装了
// example: left join {tableB} as b on a.class_id = b.id
func (w *Wrapper) Join(join string) *Wrapper {
	w.queryInfo.join += " " + join
	return w
}

// QueryUseMaster 查询时强制使用主库（默认为false）
func (w *Wrapper) QueryUseMaster(useMaster bool) *Wrapper {
	w.queryInfo.queryUseMaster = useMaster
	return w
}

func (w *Wrapper) Set(column string, value interface{}) *Wrapper {
	item := dataItem{field: column, op: "value", value: value}
	w.dataItems = append(w.dataItems, item)
	return w
}

// SetJson 将结构体格式化成json再写入数据库
func (w *Wrapper) SetJson(column string, value interface{}) *Wrapper {
	data, err := json.Marshal(value)
	if err != nil {
		err = fmt.Errorf("%s: %v(%v)", column, err, value)
		w.errList = append(w.errList, err)
		return w
	}

	item := dataItem{field: column, op: "value", value: string(data)}
	w.dataItems = append(w.dataItems, item)
	return w
}

func (w *Wrapper) SetExp(column string, value string) *Wrapper {
	item := dataItem{field: column, op: "exp", value: value}
	w.dataItems = append(w.dataItems, item)
	return w
}

func (w *Wrapper) ToSelectSql() (query string, args []interface{}, err error) {
	return getSelectSql("", w.queryInfo)
}
