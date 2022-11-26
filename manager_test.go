package sqlbp

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"math/rand"
	"os"
	"testing"
	"time"
)

// ----- constant start -----
const (
	DbMaster = "master"
	DbSlave  = "slave"
)

// ----- constant end   -----

// ----- entity start -----
const (
	TableDevStudent = "dev_student"
)

type DevStudentEntity struct {
	Id         int64     `json:"id" db:"id"`                  //
	Name       string    `json:"name" db:"name"`              // 姓名
	Age        int       `json:"age" db:"age"`                // 年龄
	ClassId    int64     `json:"classId" db:"class_id"`       // 班级ID
	CreateTime time.Time `json:"createTime" db:"create_time"` // 创建时间
}

// ----- entity end   -----

// ----- dao start -----

// 推荐与数据库相关的操作都写在dao层，外部（service, controller不直接引入go-batis-plus模块）
// 此处为了方便演示，故不遵守此约定
type DevStudentDao struct {
	BaseDao
}

// GDevStudentDao 由于golang不支持类静态函数，所以添加一个全局无状态的变量
var GDevStudentDao DevStudentDao

func init() {
	// 以下两个，必须指定，否则DB操作会出错
	GDevStudentDao.SetTableName(TableDevStudent)
	GDevStudentDao.SetDbName(DbMaster)

	// 以下的是选填，不设置的话会有默认值
	GDevStudentDao.SetSlaveDbName(DbSlave) // 默认为主库链接
}

// ----- dao end   -----

// 执行该测试用例时，请先设置环境GBP_MASTER_DSN, GBP_SLAVE_DSN，运行table中的初始化sql
func TestGoBatisPlus(t *testing.T) {
	t.Log("test ....")

	// 连接主库
	masterLink := os.Getenv("SQLBP_MASTER_DSN")
	if masterLink == "" {
		t.Errorf("dsn of master db is empty")
		return
	}
	masterDb, err := sqlx.Connect("mysql", masterLink)
	if err != nil {
		t.Error("connect error: ", err)
		return
	}

	slaveLink := os.Getenv("SQLBP_SLAVE_DSN")
	if slaveLink == "" {
		t.Errorf("dsn of slave db is empty")
		return
	}
	slaveDb, err := sqlx.Connect("mysql", slaveLink)
	if err != nil {
		t.Error("connect error: ", err)
		return
	}

	// 一般而言，生产环境上的表结构会变更，所以得开启该标志
	masterDb = masterDb.Unsafe()
	slaveDb = slaveDb.Unsafe()

	ctx := context.Background()
	linkMap := map[string]*sqlx.DB{
		DbMaster: masterDb,
		DbSlave:  slaveDb,
	}
	err = InitDbConnectMap(linkMap)

	// 单条记录的增删改查
	var affect int64
	data := DevStudentEntity{
		Name:       "test_insert",
		Age:        30,
		ClassId:    int64(rand.Int()%3 + 1),
		CreateTime: time.Now(),
	}
	id, err := GDevStudentDao.Insert(ctx, &data)
	dbLog(t, err, "insert_one, id=%d", id)

	data.Name = "test_update"
	data.Age += 100
	affect, err = GDevStudentDao.UpdateById(ctx, &data, "id", id)
	dbLog(t, err, "update_one, affect=%d", affect)

	dataMap := map[string]interface{}{
		"age": data.Age + 1,
	}
	affect, err = GDevStudentDao.UpdateById(ctx, &dataMap, "id", id)
	dbLog(t, err, "update_one_by_map, affect=%d", affect)

	err = GDevStudentDao.GetById(ctx, &data, "id", id)
	dbLog(t, err, "get_one, res=%v", data)

	affect, err = GDevStudentDao.DeleteById(ctx, "id", id)
	dbLog(t, err, "delete_one, affect=%d", affect)

	// Wrapper 单表查询
	var tGet []DevStudentEntity
	// use Eq
	tGet = make([]DevStudentEntity, 0)
	wGet := GetWrapper().Eq("age", 100)
	err = GDevStudentDao.SelectByWrapper(ctx, &tGet, wGet)
	dbLog(t, err, "select_use_eq, data=%v", tGet)

	// use In
	tGet = make([]DevStudentEntity, 0)
	wGet = GetWrapper().In("id", []int64{1, 2})
	err = GDevStudentDao.SelectByWrapper(ctx, &tGet, wGet)
	dbLog(t, err, "select_use_in, data=%v", tGet)

	// use Between
	tGet = make([]DevStudentEntity, 0)
	wGet = GetWrapper().Between("id", 1, 3)
	err = GDevStudentDao.SelectByWrapper(ctx, &tGet, wGet)
	dbLog(t, err, "select_use_between, data=%v", tGet)

	// use apply
	tGet = make([]DevStudentEntity, 0)
	wGet = GetWrapper().Apply("id = ? or id = ?", 1, 2)
	err = GDevStudentDao.SelectByWrapper(ctx, &tGet, wGet)
	dbLog(t, err, "select_use_apply, data=%v", tGet)

	mWrap := GetWrapper().Eq("age", 100)
	mMap, err := GDevStudentDao.SelectMapByWrapper(ctx, mWrap)
	dbLog(t, err, "select_map, res json=%v", mMap)

	// Wrapper 查询条数
	wCount := GetWrapper().Eq("age", 100)
	affect, err = GDevStudentDao.CountByWrapper(ctx, wCount)
	dbLog(t, err, "count_by_wrapper, count=%d", affect)

	// Wrapper 更新
	wUpdate := GetWrapper().Eq("age", 100).Set("create_time", time.Now())
	affect, err = GDevStudentDao.UpdateByWrapper(ctx, wUpdate)
	dbLog(t, err, "update_by_wrapper, affect=%d", affect)

	// Wrapper 删除，仅演示，没有删除数据(affect=0)
	wDelete := GetWrapper().Eq("age", 101)
	affect, err = GDevStudentDao.DeleteByWrapper(ctx, wDelete)
	dbLog(t, err, "delete_by_wrapper, affect=%d", affect)

	// Wrapper 联表查询
	var tJoin []struct {
		Name  string `json:"name" db:"name"`
		Count int    `json:"cn" db:"cn"`
	}
	wJoin := GetWrapper().
		Select(NullToDefaultString("c.name", ""), "count(*) as cn").
		As("s").
		Join("left join dev_class as c on s.class_id = c.id").
		Group("c.id")
	err = GDevStudentDao.SelectByWrapper(ctx, &tJoin, wJoin)
	dbLog(t, err, "join_by_wrapper, data=%v", tJoin)

	// 事务开启
	tx, err := Begin(DbMaster)
	if err != nil {
		t.Errorf("transaction begin error")
	}

	// 在主库执行事务写操作，并查看结果
	txCtx := SetCtxTransaction(ctx, tx)
	wTx := GetWrapper().Eq("age", 100)
	affect, err = GDevStudentDao.DeleteByWrapper(txCtx, wTx)
	dbLog(t, err, "delete_by_transaction, affect=%d", affect)
	wCount = GetWrapper().Eq("age", 100)
	affect, err = GDevStudentDao.CountByWrapper(txCtx, wCount)
	dbLog(t, err, "after_delete, countByWrapper=%d", affect)

	// 执行回滚
	err = Rollback(tx)
	if err != nil {
		t.Errorf("transaction rollback error")
	}
	wCount = GetWrapper().Eq("age", 100).QueryUseMaster(true)
	affect, err = GDevStudentDao.CountByWrapper(ctx, wCount)
	dbLog(t, err, "after_rollback, countByWrapper=%d", affect)

	return
}

func dbLog(t *testing.T, err error, format string, argv ...interface{}) {
	msg := fmt.Sprintf(format, argv...)
	if err != nil {
		t.Error("error: ", msg, err)
	} else {
		t.Log("success: ", msg)
	}
}
