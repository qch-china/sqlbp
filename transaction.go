package sqlbp

import (
	"context"
	"github.com/jmoiron/sqlx"
)

const (
	// 事务指针key
	ctxKeyTransactionPoint = "gbp_transaction_point"
)

func Begin(dbName string) (tx *sqlx.Tx, err error) {
	var db *sqlx.DB
	db, err = getDbConnect(dbName)
	if err != nil {
		return
	}
	tx, err = db.Beginx()
	if err != nil {
		return
	}
	return
}

func Rollback(tx *sqlx.Tx) error {
	return tx.Rollback()
}

func Commit(tx *sqlx.Tx) error {
	return tx.Commit()
}

// SetCtxTransaction 在context中设置对应的事务
// 注意：开启事务之后，SQL会在事务所在的dblink上执行，不会遵守dao的主从库设置
func SetCtxTransaction(ctx context.Context, tx *sqlx.Tx) (childCtx context.Context) {
	return context.WithValue(ctx, ctxKeyTransactionPoint, tx)
}

func GetCtxTransaction(ctx context.Context) (tx *sqlx.Tx) {
	v := ctx.Value(ctxKeyTransactionPoint)
	tx, ok := v.(*sqlx.Tx)
	if ok {
		return tx
	}
	return nil
}
