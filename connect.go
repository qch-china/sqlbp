package sqlbp

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

var (
	linkMap map[string]*sqlx.DB
	isInit  bool
)

func InitDbConnectMap(lm map[string]*sqlx.DB) error {
	if isInit {
		return fmt.Errorf("db manager is ready init")
	}
	linkMap = make(map[string]*sqlx.DB)
	for key, value := range lm {
		linkMap[key] = value
	}

	isInit = true
	return nil
}

func getDbConnect(name string) (link *sqlx.DB, err error) {
	if !isInit {
		err = fmt.Errorf("db connect is not init")
		return
	}

	link, ok := linkMap[name]
	if !ok {
		err = fmt.Errorf("this db connect(%s) is not exist", name)
		return
	}

	return
}
