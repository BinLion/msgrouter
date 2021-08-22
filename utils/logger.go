package utils

import (
	"fmt"
	"log"
	"os"
	"time"
)

var _, TimeNowZone = time.Now().Zone()

var NormalLoggerWriter = LogWriter{}

var SaramaLoggerWriter = LogWriter{}

type LogWriter struct {
	Path        string
	Prefix      string
	Fd          *os.File
	CurrentDate int64
}

// 初始化Logger
func LoggerInit(path string) {
	NormalLoggerWriter.Path = path
	NormalLoggerWriter.Prefix = "normal"

	log.SetOutput(&NormalLoggerWriter)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	SaramaLoggerWriter.Path = path
	SaramaLoggerWriter.Prefix = "sarama"
}

// 日志写入
func (this *LogWriter) Write(p []byte) (int, error) {
	this.CheckFile()
	return this.Fd.Write(p)
}

// 检查是否需要切分文件
func (this *LogWriter) CheckFile() {
	now := time.Now()
	date := (now.Unix() + int64(TimeNowZone)) / 86400

	// 如果日期没变不处理
	if date == this.CurrentDate {
		return
	}

	this.Fd.Close()

	// 创建新的日志文件
	fileName := this.Prefix + "_" + now.Format("20060102") + ".log"
	fileFullName := this.Path + "/" + fileName
	this.OpenFile(fileFullName)

	// 软链
	symlinkName := this.Path + "/" + this.Prefix + ".log"
	this.CreateSymlink(fileName, symlinkName)

	this.CurrentDate = date
	fmt.Printf("logger file create. file:%v, time:%v\n", fileName, time.Now())
}

// 打开文件
func (this *LogWriter) OpenFile(fileName string) {
	var err error
	this.Fd, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		panic(fmt.Sprintf("open log file failed. err:%s, file:%s\n", err, fileName))
	}
}

// 创建软链
func (this *LogWriter) CreateSymlink(fileName string, symlinkName string) {
	_, err := os.Lstat(symlinkName)
	if err == nil {
		os.Remove(symlinkName)
	}

	err = os.Symlink(fileName, symlinkName)
	if err != nil {
		fmt.Printf("symlink create failed. err:%s\n", err)
	}
}
