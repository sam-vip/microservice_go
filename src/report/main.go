package main

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"os"
	"os/signal"
	"syscall"
	"taole_go/report/common/container"
	"taole_go/report/common/load"
)

func main() {
	//生成容器
	cont := new(container.Container)
	timer := cron.New(cron.WithSeconds())

	go func() {
		fmt.Println("The program was start")
		r := load.New()

		if err := r.Load("../../conf.yaml"); err != nil {
			fmt.Printf("%v\n", err)
			return
		}

		_, err := r.Resolve() // 解析
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}
		r.LoadDB(cont)
		r.LoadTask(timer, cont)
	}()

	//创建监听退出chan
	ch := make(chan os.Signal)
	//监听指定信号 ctrl+c kill
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)

	for {
		s := <-ch
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			fmt.Println("The program will stop")
			timer.Stop()
			os.Exit(0)
		case syscall.SIGUSR2:
			fmt.Println("The program was reload")
			timer.Stop()
			timer.Start()
		}
	}
}
