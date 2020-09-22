/*
文件介绍：生成ltv数据
*/
package yj

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"taole_go/report/common/container"
	"taole_go/report/common/logger"
	"taole_go/report/common/opmysql"
	"time"
)

type Game struct{}

/*保存全局数据库实例*/
var dbMap map[string]*sql.DB

/*执行入口*/
func (t *Game) ActLtv(container *container.Container) {
	/*设置全局数据库实例*/
	setDB(container)

	/*设置值为1是普通模式，设置值大于1是修复模式（可以修复过去days_num天的数据）*/
	daysNum := 10

	/*获得当前时间*/
	currentTime := time.Now()

	/*执行所有天*/
	for i := 1; i <= daysNum; i++ {

		/*获取时间参数*/
		tm1 := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, currentTime.Location())
		tm2 := tm1.AddDate(0, 0, -i)
		begintime := int64(tm2.Unix())

		/*打印调试信息*/
		fmt.Println("开始执行下面这天")
		fmt.Println(tm2)
		begin := time.Now().Unix()

		/*执行当前这一天与这一天之前所有天的关联数据*/
		doCurrentDay(begintime)

		/*打印调试信息*/
		fmt.Println("父进程执行完毕")

		/*保存日志*/
		logger.Info("actLtv", logger.Field("执行", tm2), logger.Field("用时", time.Now().Unix()-begin))
	}
}

/*执行当前这一天与这一天之前所有天的关联数据*/
func doCurrentDay(begintime int64) {
	/*创建集合 | 当前这一天的之前的270天的时间戳集合 */
	regDatesMap := make(map[int]int64)
	for i := 0; i <= 270; i++ {
		if i > 30 {
			i = i + 29
		}
		regDatesMap [ i ] = begintime - (int64(i) * 86400)
	}

	for i := range regDatesMap {
		/* reg_dates_map [ i ]  是时间戳*/
		go doOneDay(regDatesMap [ i ], begintime, i)
	}
	fmt.Println("---------------当前天结束---------------")
}

/*执行第（）天前的那一天和昨天的关联数据
begintime_reg：注册开始时间
begintime_pay：支付开始时间
*/
func doOneDay(begintimeReg int64, begintimePay int64, i int) {

	/*获得当前是day几，例如day8  |  这个day变量是字符串，例如"day8" */
	day := strconv.FormatInt(int64(math.Ceil(float64(begintimePay-begintimeReg)/float64(86400))), 10)

	/*begintime_reg: 注册开始时间
	  endtime_reg: 注册结束时间*/
	endtimeReg := begintimeReg + 86400

	/*begintime_pay: 付款开始时间
	  endtime_pay: 付款结束时间*/
	endtimePay := begintimePay + 86400

	/*时间戳转化成字符传*/
	//注册开始时间
	begintimeRegStr := strconv.FormatInt(begintimeReg, 10)
	//注册结束时间
	endtimeRegStr := strconv.FormatInt(endtimeReg, 10)
	//付款开始时间
	begintimePayStr := strconv.FormatInt(begintimePay, 10)
	//付款结束时间
	endtimePayStr := strconv.FormatInt(endtimePay, 10)

	/*获得game表集合*/
	gameResult, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], "SELECT id,name FROM game")
	gameMap := make(map[string]string)
	for _, v := range *gameResult {
		gameMap [ v["id"] ] = v["name"]
	}

	/*获得注册结果集*/
	sql := "SELECT chid,ifnull(count(DISTINCT id),0)cnt,group_concat(DISTINCT id) uids FROM reg  WHERE `instime` between " + begintimeRegStr + " and " + endtimeRegStr + " group by chid"
	rows, _ := opmysql.FetchRows(dbMap["yj_source_admin"], sql)
	if rows == nil {
		return
	}

	/*循环注册结果集 |  处理这些人第day()天的付款结果*/
	for _, v := range *rows {

		gid, cid, cname := "0", "0", ""
		sql = "SELECT channel.cid, channel.cname, channel.gid FROM putin left join channel on channel.cid=putin.cid WHERE putin.id=" + v[ "chid" ] + " LIMIT 1"
		channel, _ := opmysql.FetchRows(dbMap["yj_source_taole666"], sql)
		if len(*channel) > 0 {
			gid, cid, cname = (*channel)[ 0 ][ "gid" ], (*channel)[ 0 ][ "cid" ], (*channel)[ 0 ][ "cname" ]
		}

		/*得到累计付费人数 和累计付费金额*/
		where := "uid in (" + v["uids"] + ") and `updtime` > " + begintimeRegStr + " and updtime<" + endtimePayStr
		sql = "select count(distinct(uid)) num_total" + day + ", sum(amount) money_total" + day + " from pay where " + where
		rsTotal, _ := opmysql.FetchRows(dbMap["yj_source_admin"], sql)

		/*得到按天付费人数 和按天付费金额*/
		where = "uid in (" + v["uids"] + ") and `updtime` > " + begintimePayStr + " and updtime<" + endtimePayStr
		sql = "select count(distinct(uid)) num" + day + ", sum(amount) money" + day + " from pay where " + where
		rsDay, _ := opmysql.FetchRows(dbMap["yj_source_admin"], sql)

		/*处理字段值*/
		if (*rsDay)[0]["num"+day] == "NULL" {
			(*rsDay)[0]["num"+day] = "0"
		}
		if (*rsDay)[0]["money"+day] == "NULL" {
			(*rsDay)[0]["money"+day] = "0"
		}
		if (*rsTotal)[0]["num_total"+day] == "NULL" {
			(*rsTotal)[0]["num_total"+day] = "0"
		}
		if (*rsTotal)[0]["money_total"+day] == "NULL" {
			(*rsTotal)[0]["money_total"+day] = "0"
		}

		/*获得需要保存到统计表的MAP集合*/
		var data  = make(map[string]string)
		data [ "ymd" ] = string(time.Unix(begintimeReg, 0).Format("20060102"))
		data [ "gid" ] = gid
		data [ "cid" ] = cid
		data [ "cname" ] = cname
		data [ "gname" ] = gameMap[ gid ]
		data [ "reg_num_total" ] = v["cnt"]
		data [ "chid" ] = v["chid"]
		data [ "num"+day ] = (*rsDay)[ 0 ][ "num"+day ]
		data [ "money"+day ] = (*rsDay)[ 0 ][ "money"+day ]
		data [ "num_total"+day ] = (*rsTotal)[ 0 ][ "num_total"+day ]
		data [ "money_total"+day ] = (*rsTotal)[ 0 ][ "money_total"+day ]

		isHasData, _ := opmysql.FetchRows(dbMap["yj_stat"], "SELECT id FROM ltv_base WHERE ymd="+data [ "ymd" ]+" AND chid="+data ["chid"]+" LIMIT 1")

		/*如果统计表不存在数据，就执行插入*/
		if len(*isHasData) == 0 {

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
				if (i == "cname") || (i == "gname") {
					data[ i ] = "'" + data[ i ] + "'"
				}
				columns += i
				values += data[ i ]
			}
			sql = "INSERT INTO ltv_base (" + columns + ") VALUES(" + values + ")"

			/*sql执行*/
			logger.Info("actLtvSql", logger.Field("执行", sql))
			result, err := opmysql.Insert(dbMap[ "yj_stat" ], sql)
			logger.Info("actLtvSql", logger.Field("执行结果:", result))
			if err != nil{
				logger.Error("actLtvSql", logger.Field("sql执行异常情况:", err))
			}

			/*如果统计表有数据，则update更新*/
		} else {

			/*sql拼接*/
			sql = "UPDATE ltv_base SET "
			tag := 1
			for i := range data {

				if tag != 1 {
					sql += ","
				}
				tag = 2

				if (i == "cname") || (i == "gname") {
					data[ i ] = "'" + data[ i ] + "'"
				}
				sql += i + "=" + data[ i ]
			}

			/*sql执行*/
			sql += " WHERE ymd=" + data [ "ymd" ] + " AND chid=" + data [ "chid" ]

			logger.Info("actLtvSql", logger.Field("执行", sql))
			result, err := opmysql.Exec(dbMap[ "yj_stat" ], sql)
			logger.Info("actLtvSql", logger.Field("执行结果:", result))
			if err != nil{
				logger.Error("actLtvSql", logger.Field("sql执行异常情况:", err))
			}
		}
	}

	fmt.Println("子进程" + strconv.FormatInt( int64(i), 10 ) + "执行完毕")
}

/*保存全局数据库实例到集合*/
func setDB(container *container.Container) {

	if len(dbMap) == 0 {
		var ok bool
		dbMap = make(map[string]*sql.DB)

		dbMap[ "yj_source_admin" ], ok = container.GetMysql("yj_source_admin")
		if ok == false {
			panic("mysql is err")
		}

		dbMap[ "yj_source_taole666" ], ok = container.GetMysql("yj_source_taole666")
		if ok == false {
			panic("mysql is err")
		}

		dbMap[ "yj_stat" ], ok = container.GetMysql("yj_stat")
		if ok == false {
			panic("mysql is err")
		}

		dbMap[ "yj_source_supersdk" ], ok = container.GetMysql("yj_source_supersdk")
		if ok == false {
			panic("mysql is err")
		}
	}
}
