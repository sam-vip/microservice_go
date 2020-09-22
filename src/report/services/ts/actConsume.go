package ts

import (
	"taole_go/report/common/container"
	"taole_go/report/common/logger"
	"taole_go/report/common/opmysql"
)

type Game struct{}

func (t *Game) ActConsume(container *container.Container) {
	db, ok := container.GetMysql("taoshou")
	if ok == false {
		panic("mysql is err")
	}

	rows, _ := opmysql.FetchRows(db, "SELECT platform,device FROM client_account limit 1000")

	for _, v := range *rows {
		logger.Error("console",logger.Field("platform",v["platform"]),logger.Field("device",v["device"]))
	}

	//row, _ := opmysql.FetchRow(db, "SELECT platform,device FROM client_account where id = 100")
	//
	//fmt.Println(*row)

	//fmt.Println("1231231231321213")
}
