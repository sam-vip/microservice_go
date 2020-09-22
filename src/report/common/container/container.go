package container

import (
	"database/sql"
	"sync"
)

//简单的容器
type Container struct {
	mux    sync.RWMutex
	mysql  map[string]*sql.DB
}

//获取容器操作
func (c *Container) GetMysql(name string) (mysql *sql.DB, ok bool) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	mysql, ok = c.mysql[name]
	return mysql, ok
}

//添加容器内容
func (c *Container) AddMsql(name string, mysql *sql.DB) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.mysql == nil {
		c.mysql = make(map[string]*sql.DB)
	}
	c.mysql[name] = mysql
}
