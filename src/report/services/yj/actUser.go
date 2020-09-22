/*
文件介绍：
1.同步美安user表数据到统计库
2.补全美安后台的user表的aid
*/
package yj

import (
	"strings"
	"taole_go/report/common/container"
	"taole_go/report/common/logger"
	"taole_go/report/common/opmysql"
	"time"
)

/*执行入口 - user表同步*/
func (t *Game) ActUser(container *container.Container) {
	/*设置全局数据库实例*/
	setDB(container)

	/*打印调试信息*/
	logger.Info("开始执行")
	begin := time.Now().Unix()

	/*同步user表数据*/
	//syncUserTable()

	/*同步aid*/
	syncAid()

	/*打印调试信息*/
	logger.Info("执行结束,用时：")

	/*保存日志*/
	logger.Info("actUser", logger.Field("actUser执行同步user表", ""), logger.Field("用时:", time.Now().Unix()-begin))
}

func syncAid() {

	/*获得aid为0的数据，放入uid_str*/
	sql1 := "SELECT id FROM `user` WHERE aid =0"
	rows1, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], sql1)

	count := 0
	uid_str := ""
	for _, data := range *rows1 {
		count++
		if count > 1 {
			uid_str += ","
		}
		uid_str += data[ "id" ]
	}

	/*到运营库查询aid*/
	sql2 := "SELECT sdk_uid,aid FROM sdk_thd_login WHERE sdk_uid IN(" + uid_str + ") AND qudao IN('THDAND4201','THDIOS4202','THDAND4301')"
	//logger.Info(sql2)
	rows2, _ := opmysql.FetchRows(dbMap["yj_source_supersdk"], sql2)

	for _, data := range *rows2 {

		aid := data[ "aid" ]
		sdk_uid := data[ "sdk_uid" ]

		if sdk_uid != "0" && sdk_uid != "" {
			/*补全美安后台的user表的aid*/
			sql3 := "UPDATE user SET aid = " + aid + " WHERE id = " + sdk_uid
			logger.Info(sql3)
			_, err := opmysql.Exec(dbMap[ "yj_source_taole666" ], sql3)
			if err != nil {
				logger.Error("syncAid", logger.Field("sql执行异常SQL:", sql3))
				logger.Error("syncAid", logger.Field("sql执行异常错误:", err))
			} else {
				logger.Info("syncAid", logger.Field("sql执行成功SQL:", sql3))
			}
		}
	}

}

func syncUserTable() {

	row, _ := opmysql.FetchRows(dbMap["yj_stat"], "SELECT id FROM `meian_user` ORDER BY id DESC LIMIT 1")
	maxId := "0"
	if len(*row) > 0 {
		maxId = (*row)[ 0 ][ "id" ]
	}

	sql := "SELECT id, aid, username, password, nickname, gender, instime, token, subscribe, fuid, chid, type, logflag, " +
		"JSON_EXTRACT(extend, '$.devid') devid, ifnull(JSON_EXTRACT(extend, '$.ocpc_id'),0) ocpc_id FROM user WHERE id >" + maxId
	logger.Info(sql)
	rows, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], sql)

	if len(*rows) > 0 {
		insertUser(rows)
	}

	logger.Info("同步user表到统计库：执行完毕")
}

func insertUser(rows *[]map[string]string) {

	for _, data := range *rows {
		if data[ "aid" ] == "0" {
			/*获得aid*/
			getAid, _ := opmysql.FetchRows(dbMap["yj_source_supersdk"], "SELECT aid FROM `sdk_thd_login` WHERE qudao IN('THDAND4201','THDIOS4202') AND sdk_uid = "+data[ "id" ]+" LIMIT 1")
			if len(*getAid) > 0 {
				if (*getAid)[0][ "aid" ] != "0" {
					data[ "aid" ] = (*getAid)[0][ "aid" ]

					/*顺便处理另外一件事情：补全美安后台的user表的aid*/
					sql := "UPDATE user SET aid = " + data[ "aid" ] + " WHERE id = " + data[ "id" ]
					_, err := opmysql.Exec(dbMap[ "yj_source_taole666" ], sql)
					if err != nil {
						logger.Error("actUserSql", logger.Field("sql执行异常情况:", err))
					}
				}
			}
		}

		/*数据拼接 | 申明 - 需要拼接的字段*/
		data[ "gid" ], data[ "cid" ], data[ "gname" ], data[ "cname" ], data[ "pname" ] = "", "", "", "", ""

		/*数据拼接 | 获得 - 需要拼接的字段*/
		/*获得广告*/
		getPutin, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT id, gid, cid, name FROM `putin` WHERE id = "+data[ "chid" ]+" LIMIT 1")
		if len(*getPutin) > 0 {

			data[ "pname" ] = (*getPutin)[ 0 ][ "name" ]
			data[ "gid" ] = (*getPutin)[ 0 ][ "gid" ]
			data[ "cid" ] = (*getPutin)[ 0 ][ "cid" ]

			/*获得游戏*/
			getGame, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT id, name FROM `game` WHERE id = "+data[ "gid" ]+" LIMIT 1")
			if len(*getGame) > 0 {
				data[ "gname" ] = (*getGame)[ 0 ][ "name" ]
			}

			/*获得渠道*/
			getChannel, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT cname FROM `channel` WHERE cid = "+data[ "cid" ]+" LIMIT 1")
			if len(*getChannel) > 0 {
				data[ "cname" ] = (*getChannel)[ 0 ][ "cname" ]
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

			if (i == "ocpc_id" || i == "devid") {
				data[ i ] = strings.Replace(data[ i ], "\"", "", -1)
			}
			values += "'" + data[ i ] + "'"
		}

		sql := "INSERT INTO meian_user (" + columns + ") VALUES(" + values + ")"

		/*sql执行*/
		logger.Info("actUserSql", logger.Field("执行", sql))
		result, err := opmysql.Insert(dbMap[ "yj_stat" ], sql)
		logger.Info("actUserSql", logger.Field("执行结果:", result))
		if err != nil {
			logger.Error("actUserSql", logger.Field("sql执行异常情况:", err))
		}
	}
}
