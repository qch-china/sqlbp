package sqlbp

import (
	"context"
	"fmt"
)

type BaseDao struct {
	tableName   string // 表名
	dbName      string // 主库连接名
	slaveDbName string // 从库连接名（没设置则查询使用主库）
}

func (dao *BaseDao) SetTableName(table string) {
	dao.tableName = table
}

func (dao *BaseDao) SetDbName(name string) {
	dao.dbName = name
}

func (dao *BaseDao) SetSlaveDbName(name string) {
	dao.slaveDbName = name
}

func (dao *BaseDao) GetTableName() string {
	return dao.tableName
}

func (dao *BaseDao) GetDbName() string {
	return dao.dbName
}

func (dao *BaseDao) GetSlaveDbName() string {
	if dao.slaveDbName == "" {
		return dao.dbName
	} else {
		return dao.slaveDbName
	}
}

func (dao *BaseDao) CheckDao() error {
	if dao.tableName == "" || dao.dbName == "" {
		return fmt.Errorf("table name or db name is not allow empty")
	}
	return nil
}

func (dao *BaseDao) Insert(
	ctx context.Context,
	data interface{},
) (lastId int64, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	dataItems, err := structToDataItems(data, "db", "")
	if err != nil {
		return
	}

	w := GetWrapper()
	w.dataItems = dataItems
	return insertByWrapper(ctx, dao, w)
}

// UpdateById 更新数据
func (dao *BaseDao) UpdateById(
	ctx context.Context,
	data interface{},
	idKey string,
	id interface{},
) (affectedRow int64, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	w := GetWrapper()
	dataItems, err := structToDataItems(data, "db", idKey)
	if err != nil {
		return
	}

	w.queryInfo.where = []whereItem{createWhereItem(idKey, "=", id)}
	w.dataItems = dataItems
	return updateByWrapper(ctx, dao, w)
}

// UpdateByWrapper 更新数据
func (dao *BaseDao) UpdateByWrapper(
	ctx context.Context,
	w *Wrapper,
) (affectedRow int64, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	err = w.GetError()
	if err != nil {
		return
	}
	return updateByWrapper(ctx, dao, w)
}

// DeleteById 删除数据
func (dao *BaseDao) DeleteById(
	ctx context.Context,
	idKey string,
	id interface{},
) (affectedRow int64, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	w := GetWrapper()
	w.queryInfo.where = []whereItem{createWhereItem(idKey, "=", id)}
	return deleteByWrapper(ctx, dao, w)
}

// DeleteByWrapper 更新数据
func (dao *BaseDao) DeleteByWrapper(
	ctx context.Context,
	w *Wrapper,
) (affectedRow int64, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	err = w.GetError()
	if err != nil {
		return
	}
	return deleteByWrapper(ctx, dao, w)
}

// SelectByWrapper 执行多条数据的查询请求
func (dao *BaseDao) SelectByWrapper(
	ctx context.Context,
	dest interface{},
	w *Wrapper,
) (err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	err = w.GetError()
	if err != nil {
		return
	}
	return selectByWrapper(ctx, dest, dao, w)
}

// SelectMapByWrapper 执行多条数据的查询请求，生成[]map[string]interface{}
func (dao *BaseDao) SelectMapByWrapper(
	ctx context.Context,
	w *Wrapper,
) (result []map[string]interface{}, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	err = w.GetError()
	if err != nil {
		return
	}
	return selectMapByWrapper(ctx, dao, w)
}

// GetById 通过ID查询单条记录（ID可以是表中的任何字段）
func (dao *BaseDao) GetById(
	ctx context.Context,
	dest interface{},
	idKey string,
	id interface{},
) (err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	return getOneData(ctx, dao, dest, idKey, id)
}

// CountByWrapper 执行多条数据的查询请求
func (dao *BaseDao) CountByWrapper(
	ctx context.Context,
	w *Wrapper,
) (result int64, err error) {
	err = dao.CheckDao()
	if err != nil {
		return
	}

	err = w.GetError()
	if err != nil {
		return
	}
	return countByWrapper(ctx, dao, w)
}
