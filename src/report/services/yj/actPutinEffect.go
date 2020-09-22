/*
文件介绍：
1.根据美安的多个统计表，生成广告活动数据概览统计表meian_putin_effect的表数据
*/
package yj

import (
	"fmt"
	"strings"
	"taole_go/report/common/container"
	"taole_go/report/common/logger"
	"taole_go/report/common/opmysql"
	"time"
)

/*执行入口 - user表同步*/
func (t *Game) ActPutinEffect(container *container.Container) {
	/*设置全局数据库实例*/
	setDB(container)

	/*打印调试信息*/
	fmt.Println("开始执行")
	begin := time.Now().Unix()

	/*同步meian_putin_effect表数据*/
	syncPutinEffectTable()

	/*打印调试信息*/
	fmt.Println("执行结束,用时：")
	fmt.Println(time.Now().Unix() - begin)

	/*保存日志*/
	logger.Info("actPutinEffect", logger.Field("ActPutinEffect执行同步meian_putin_effect表", ""), logger.Field("用时:", time.Now().Unix()-begin))
}

/*同步meian_putin_effect表数据*/
func syncPutinEffectTable() {

	/*以统计库最大的日期为分界，同步源库的数据*/
	row, _ := opmysql.FetchRows(dbMap["yj_stat"], "SELECT ymd FROM `meian_putin_effect` ORDER BY id DESC LIMIT 1")
	maxYmd := "0"
	if len(*row) > 0 {
		maxYmd = (*row)[ 0 ][ "ymd" ]
	}

	/*以下逻辑是综合3表数据到一张表(三表：cnt_active_game,cnt_pay_game,keep)*/
	/*获得cnt_active_game表统计项*/
	sql1 := "SELECT ymd, chid, SUM(cntatv) AS cntatv,SUM(cntdl) AS cntdl,SUM(cntreg) AS cntreg FROM `cnt_active_game` WHERE `h` = 24 AND ymd>" + maxYmd + " GROUP BY ymd,chid"
	fmt.Println(sql1)
	rows1, _ := opmysql.FetchRows(dbMap["yj_source_admin"], sql1)

	if len(*rows1) > 0 {
		updateTablePutinEffect(rows1)
	}

	/*获得cnt_pay_game表统计项*/
	sql2 := "SELECT ymd, chid, SUM(amount) amount,SUM(persons) persons,SUM(nps) nps,SUM(nfs) nfs FROM `cnt_pay_game` WHERE `h` = 24 AND `sid` = 0 AND ymd>" + maxYmd + " GROUP BY ymd,chid"
	fmt.Println(sql2)
	rows2, _ := opmysql.FetchRows(dbMap["yj_source_admin"], sql2)

	if len(*rows2) > 0 {
		updateTablePutinEffect(rows2)
	}

	/*获得keep表统计项*/
	sql3 := "SELECT ymd, chid, cnt , " +
		"ifnull(JSON_EXTRACT(extend, '$.d1'), 0) day1, " +
		"ifnull(JSON_EXTRACT(extend, '$.d3'), 0) day3, " +
		"ifnull(JSON_EXTRACT(extend, '$.d7'), 0) day7, " +
		"ifnull(JSON_EXTRACT(extend, '$.d30'), 0) day30, " +
		"ifnull(JSON_EXTRACT(extend, '$.d60'), 0) day60, " +
		"ifnull(JSON_EXTRACT(extend, '$.d90'), 0) day90 " +
		"FROM `keep` WHERE ymd>" + maxYmd
	fmt.Println(sql3)
	rows3, _ := opmysql.FetchRows(dbMap["yj_source_admin"], sql3)

	if len(*rows3) > 0 {
		updateTablePutinEffect(rows3)
	}

	fmt.Println("同步meian_putin_effect表到统计库：执行完毕")
}


func updateTablePutinEffect(rows *[]map[string]string) {

	for _, data := range *rows {

		/*数据拼接 | 申明 - 需要拼接的字段*/
		data[ "gid" ], data[ "cid" ] = "0", "0"

		/*数据拼接 | 获得 - 需要拼接的字段*/
		/*获得广告*/
		getPutin, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT id, gid, cid, name FROM `putin` WHERE id = " + data[ "chid" ] + " LIMIT 1")
		if len(*getPutin) > 0 {

			data[ "gid" ] = (*getPutin)[ 0 ][ "gid" ]
			data[ "cid" ] = (*getPutin)[ 0 ][ "cid" ]
		}

		sql := "SELECT id FROM `meian_putin_effect` WHERE ymd = " + data[ "ymd" ] + " AND chid = " + data[ "chid" ] + " LIMIT 1"
		rows, _ := opmysql.FetchRows(dbMap["yj_stat"], sql)
		/*如果存在，则更新*/
		if len(*rows) > 0 {
			/*sql拼接*/
			sql = "UPDATE meian_putin_effect SET "
			tag := 1
			for i := range data {

				if tag != 1 {
					sql += ","
				}
				tag = 2

				sql += i + "=" + data[ i ]
			}

			/*sql执行*/
			sql += " WHERE ymd=" + data [ "ymd" ] + " AND chid=" + data [ "chid" ]

			logger.Info("actPutinEffectSql", logger.Field("执行", sql))
			result, err := 	opmysql.Exec(dbMap[ "yj_stat" ], sql)
			logger.Info("actPutinEffectSql", logger.Field("执行结果:", result))
			if err != nil{
				logger.Error("actPutinEffectSql", logger.Field("sql执行异常情况:", err))
			}

			/*如果不存在，则插入*/
		}else{
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
				if (i == "day1" || i == "day3" || i == "day7" || i == "day30" || i == "day60" || i == "day90") {
					data[ i ] = strings.Replace(data[ i ], "\"", "", -1)
				}
				values += "'" + data[ i ] + "'"
			}
			sql := "INSERT INTO meian_putin_effect (" + columns + ") VALUES(" + values + ")"

			/*sql执行*/
			logger.Info("actPutinEffectSql", logger.Field("执行", sql))
			result, err := 	opmysql.Insert(dbMap[ "yj_stat" ], sql)
			logger.Info("actPutinEffectSql", logger.Field("执行结果:", result))
			if err != nil{
				logger.Error("actPutinEffectSql", logger.Field("sql执行异常情况:", err))
			}
		}
	}
}