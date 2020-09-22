package load

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"path"
	"path/filepath"
	"reflect"
	"taole_go/report/common/container"
	"taole_go/report/common/logger"
	"taole_go/report/services/ts"
	"taole_go/report/services/yj"
	"time"
)

// DB数据库配置
type DB struct {
	Host            string `yaml:"host"`
	Port            string `yaml:"port"`
	Dbname          string `yaml:"dbname"`
	Username        string `yaml:"username"`
	Password        string `yaml:"password"`
	Connmaxlifetime int    `yaml:"connmaxlifetime"`
	Maxopenconns    int    `yaml:"maxopenconns"`
}

// Redis配置
type Redis struct {
	Expire   string `yaml:"expire"`
	Select   string `yaml:"select"`
	Host     string `yaml:"host"`
	Dbname   string `yaml:"dbname"`
	Password string `yaml:"password"`
}

// 任务配置
type Config struct {
	Mysql map[string]DB                `yaml:"mysql"`
	Redis map[string]Redis             `yaml:"redis"`
	Task  map[string]map[string]string `yaml:"task"`
}

// Resolver 解析器
type Resolver struct {
	filename string
	Conf     *Config
	in       []byte
}

// Manager 管理器 集成三方解析api
var Manager = map[string]func([]byte, interface{}) error{
	"yaml": yaml.Unmarshal,
	"json": json.Unmarshal,
	"xml":  xml.Unmarshal,
}

// New 初始化实例
func New() *Resolver {
	return &Resolver{Conf: new(Config)}
}

// Name 文件名
func (r *Resolver) Name() string {
	return filepath.Base(r.filename)
}

// Type 类型
func (r *Resolver) Type() string {
	return path.Ext(r.filename)[1:]
}

// Load 加载
func (r *Resolver) Load(filename string) (err error) {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("加载错误: %v ", err)
	}
	r.filename = filename
	r.in = raw
	return nil
}

// Resolve 解析
func (r *Resolver) Resolve() (out interface{}, err error) {
	ext := r.Type()
	if unmarshal, ok := Manager[ext]; ok {
		err = unmarshal(r.in, r.Conf)
	}
	if err != nil {
		return nil, errors.New("解析错误")
	}

	out = *r.Conf
	return
}

// DB加载
func (r *Resolver) LoadDB(c *container.Container) {
	for k, v := range r.Conf.Mysql {
		connInfo := v.Username + ":" + v.Password + "@tcp(" + v.Host + ":" + v.Port + ")/" + v.Dbname + "?charset=utf8"
		db, err := sql.Open("mysql", connInfo)
		if err != nil {
			logger.Error("mysql connect error",logger.Field("err",err))
		}
		db.SetConnMaxLifetime(time.Duration(v.Connmaxlifetime) * time.Second)
		db.SetMaxOpenConns(int(v.Maxopenconns))
		c.AddMsql(k, db)
	}
}

// 加载任务
func (r *Resolver) LoadTask(crontab *cron.Cron, cont *container.Container) {
	for game, info := range r.Conf.Task {
		for behevior, corntabConfig := range info {
			var gameObj interface{}
			switch game {
			case "ts":
				gameObj = new(ts.Game)
			case "yj":
				gameObj = new(yj.Game)
			}
			behevior := behevior
			_, err := crontab.AddFunc(corntabConfig, func() {
				getValue := reflect.ValueOf(gameObj)
				methodValue := getValue.MethodByName(behevior)
				args := []reflect.Value{reflect.ValueOf(cont)}
				methodValue.Call(args)
				//reflect.ValueOf(gameObj).MethodByName(behevior).Call(nil)
			})
			if err != nil {
				logger.Error("load task error",logger.Field("err",err),
					logger.Field("game",game),
					logger.Field("corntabConfig",corntabConfig),
					logger.Field("behevior",behevior))
			}
		}
	}

	defer crontab.Stop()
	go crontab.Start()
}

func (r *Resolver)LoadLogger(cont *container.Container)  {

}
