/*
文件介绍：同步美安pay表数据到统计库
*/
package yj

import (
	"fmt"
	"taole_go/report/common/container"
	"taole_go/report/common/logger"
	"taole_go/report/common/opmysql"
	"time"
)

/*执行入口 - pay表同步*/
func (t *Game) ActPay(container *container.Container) {
	/*设置全局数据库实例*/
	setDB(container)

	/*打印调试信息*/
	fmt.Println("开始执行")
	begin := time.Now().Unix()

	/*同步pay表数据*/
	syncPayTable()

	/*打印调试信息*/
	fmt.Println("执行结束,用时：")
	fmt.Println(time.Now().Unix() - begin)

	/*保存日志*/
	logger.Info("actPay", logger.Field("actPay执行同步pay表", ""), logger.Field("用时:", time.Now().Unix()-begin))
}

/*同步pay表数据*/
func syncPayTable() {

	/*查询统计库最大的主键*/
	row, _ := opmysql.FetchRows(dbMap["yj_stat"], "SELECT pk FROM `meian_pay` ORDER BY pk DESC LIMIT 1")
	maxPk := "0"
	if len(*row) > 0 {
		maxPk = (*row)[ 0 ][ "pk" ]
	}

	/*以统计库最大的主键为分界，同步源库的数据*/
	rows, _ := opmysql.FetchRows(dbMap["yj_source_admin"], "	SELECT * FROM pay WHERE pk >" + maxPk)

	/*执行同步*/
	if len(*rows) > 0 {
		insertPay(rows)
	}

	fmt.Println("同步pay表到统计库：执行完毕")
}

func insertPay(rows *[]map[string]string) {

	for _, data := range *rows {

		/*数据拼接 | 申明 - 需要拼接的字段*/
		data[ "aid" ], data[ "cid" ], data[ "gname" ], data[ "cname" ], data[ "pname" ], data[ "uname" ], data[ "regtime" ]  =  "0", "0", "", "", "", "", "0"

		/*数据拼接 | 获得 - 需要拼接的字段*/
		/*获得广告*/
		getPutin, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT id, cid, name FROM `putin` WHERE id = " + data[ "chid" ] + " LIMIT 1")
		if len(*getPutin) > 0 {

			data[ "cid" ] = (*getPutin)[ 0 ][ "cid" ]
			data[ "pname" ] = (*getPutin)[ 0 ][ "name" ]

			/*获得游戏*/
			getGame, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT id, name FROM `game` WHERE id = " + data[ "gid" ] + " LIMIT 1")
			if len(*getGame) > 0 {
				data[ "gname" ] = (*getGame)[ 0 ][ "name" ]
			}

			/*获得渠道*/
			getChannel, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT cname FROM `channel` WHERE cid = " + data[ "cid" ] + " LIMIT 1")
			if len(*getChannel) > 0 {
				data[ "cname" ] = (*getChannel)[ 0 ][ "cname" ]
			}

			/*获得用户*/
			getUser, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT aid, username, instime FROM `user` WHERE id = " + data[ "uid" ] + " LIMIT 1")
			if len(*getUser) > 0 {
				data[ "aid" ] = (*getUser)[ 0 ][ "aid" ]
				data[ "uname" ] = (*getUser)[ 0 ][ "username" ]
				data[ "regtime" ] = (*getUser)[ 0 ][ "instime" ]
			}
		}

		/*sql拼接*/
		tag := 1
		columns := ""
		values := ""
		for i := range data {
			if tag != 1 {
				columns += ","
				values += ","
			}
			tag = 2
			columns += i
			if (i == "gname" || i == "cname" || i == "pname" || i == "uname") {
				data[ i ] = "'" + data[ i ] + "'"
			}
			values += data[ i ]
		}

		sql := "INSERT INTO meian_pay (" + columns + ") VALUES(" + values + ")"

		/*sql执行*/
		logger.Info("actPaySql", logger.Field("执行", sql))
		result, err := 	opmysql.Insert(dbMap[ "yj_stat" ], sql)
		logger.Info("actPaySql", logger.Field("执行结果:", result))
		if err != nil{
			logger.Error("actPaySql", logger.Field("sql执行异常情况:", err))
		}
	}
}


