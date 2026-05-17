package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"time"
)

var DefaultDownloadPath = "C:\\Users\\imdxh\\Downloads"
var ch chan string = make(chan string)
var IDs sync.Map
var Retrychan chan string = make(chan string)
var MaxID int = 0
var Limiter chan struct{} = make(chan struct{}, 5)
var retryLimit sync.Map
var waitqueue chan Request = make(chan Request, 5)

type Record struct {
	ID       int    `json:"ID"`
	Status   string `json:"Status"`
	URL      string `json:"URL"`
	Filename string `json:"Filename"`
}

type Request struct {
	URL      string
	Filename string
	id       int
	flag     bool
}

func DownloadFile(url string, filename string, id int, flag bool) (err error) {
	HandleStatus("下载中...", id, flag, url, filename)
	var Done chan string = make(chan string)
	go func() {
		timer := time.NewTimer(5 * time.Hour)
		defer timer.Stop()
		select {
		case <-timer.C:
			HandleStatus("下载超时", id, flag, url, filename)
			return
		case <-Done:
			select {
			case req := <-waitqueue:
				Limiter <- struct{}{}
				go DownloadFile(req.URL, req.Filename, req.id, req.flag)
			default:
			}
			return
		}
	}()
	if count, exist := retryLimit.Load(url); exist {
		fmt.Printf("重试次数为%d", count)
		if count.(int) >= 3 {
			fmt.Printf("重试太多了！！！")
			HandleStatus("下载失败，超过重试次数,请5分钟后重试", id, flag, url, filename)
			go func() {
				timer := time.NewTimer(5 * time.Second) // 5分钟后重置重试次数,避免一直重试
				<-timer.C
				retryLimit.Delete(url)
			}()
			<-Limiter
			return nil
		} else {
			retryLimit.Store(url, count.(int)+1)
		}
	} else {
		retryLimit.Store(url, 1)
	}
	resp := &http.Response{}
	resp, err = http.Get(url)
	if err != nil {
		ch <- err.Error()
		Done <- "下载失败"
		<-Limiter
		HandleStatus("下载失败", id, flag, url, filename)
		return err
	}
	defer resp.Body.Close()
	data := &os.File{}
	data, err = os.Create(DefaultDownloadPath + "\\" + filename)
	if err != nil {
		ch <- err.Error()
		Done <- "下载失败"
		<-Limiter
		HandleStatus("下载失败", id, flag, url, filename)
		return err
	}
	defer data.Close()
	_, err = io.Copy(data, resp.Body)
	if err != nil {
		ch <- err.Error()
		Done <- "下载失败"
		<-Limiter
		HandleStatus("下载失败", id, flag, url, filename)
		return err
	}
	nowTime := time.Now()
	length, err := os.Stat(DefaultDownloadPath + "\\" + filename)
	if err != nil {
		ch <- err.Error()
		Done <- "下载失败"
		<-Limiter
		HandleStatus("下载失败", id, flag, url, filename)
		return err
	}
	HandleStatus("文件下载成功"+",文件大小为"+strconv.Itoa(int(length.Size()))+"字节,下载耗时"+time.Since(nowTime).String()+"，文件来源URL："+url, id, flag, url, filename)
	ch <- "下载完成"
	Done <- "下载完成"
	<-Limiter
	return nil
}

func HandleDownload() {
	var url string
	var filename string
	for {
		var flag string
		fmt.Println("输入yes开始下载，输入list查看历史下载记录，输入quit退出")
		fmt.Scan(&flag)
		if flag == "quit" {
			break
		} else if flag == "list" {
			HandleList()
			continue
		}
		fmt.Printf("请输入下载地址和保存的文件名")
		_, err := fmt.Scan(&url, &filename)
		if err != nil {
			fmt.Println("输入错误，请重新输入")
			continue
		}
		if url == "" {
			fmt.Println("下载地址不能为空，请重新输入")
			continue
		}
		if filename == "" {
			filename = url[strings.LastIndex(url, "/")+1:] // 获取url最后一个/后面的字符串
		}
		MaxID++
		id := MaxID
		_, Iflag := IDs.Load(id)
		select {
		case Limiter <- struct{}{}:
		default:
			if len(waitqueue) == 5 {
				HandleStatus("下载请求太多了请稍候再试", id, Iflag, url, filename)
				continue
			}
			waitqueue <- Request{url, filename, MaxID, false}
			HandleStatus("下载文件过多已将文件加入等待队列，稍后开始下载", id, Iflag, url, filename)
			continue
		}
		go func() {
			IDs.Store(id, url+","+filename)
			HandleStatus("开始下载", id, Iflag, url, filename)
			Iflag = true
			_ = DownloadFile(url, filename, id, Iflag)
		}()
	}
}

func init() {
	data, err := os.ReadFile("history.json")
	if err != nil {
		panic(err)
	}
	var records []Record
	json.Unmarshal(data, &records)
	if len(records) > 0 {
		MaxID = records[len(records)-1].ID
	}
	for _, record := range records {
		IDs.Store(record.ID, record.URL+","+record.Filename)
	}
}

func HandleStatus(status string, id int, flag bool, url string, filename string) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	data, err := os.ReadFile("history.json")
	if err != nil {
		panic(err)
	}
	var records []Record
	if len(data) > 0 {
		err = json.Unmarshal(data, &records)
		if err != nil {
			panic(err)
		}
	}
	if !flag {
		records = append(records, Record{id, status, url, filename})
	} else {
		for i, record := range records {
			if record.ID == id {
				records[i].Status = status
				break
			}
		}
	}
	data, err = json.MarshalIndent(records, "", "")
	if err != nil {
		panic(err)
	}
	err = os.WriteFile("history.json", data, 0644)
	if err != nil {
		panic(err)
	}
}

func HandleRetry(url string, filename string, id int, flag bool) {
	Limiter <- struct{}{}
	go func() {
		fmt.Printf("开始重试下载文件:%s\n", filename)
		err := DownloadFile(url, filename, id, flag)
		fmt.Printf("重试结束!!!")
		if err != nil {
			HandleStatus("重试失败", id, flag, url, filename)
		}
	}()
}

func HandleList() {
	data, err := os.ReadFile("history.json")
	if err != nil {
		panic(err)
	}
	var records []Record
	err = json.Unmarshal(data, &records)
	if err != nil {
		panic(err)
	}
	for _, record := range records {
		fmt.Printf("ID: %d, Status: %s\n", record.ID, record.Status)
	}
	var id string
	fmt.Println("输入ID重试下载，输入quit退出")
	fmt.Scan(&id)
	if id == "quit" {
		return
	} else {
		Uid, _ := strconv.Atoi(id)
		// fmt.Println(Uid)
		if Rid, exist := IDs.Load(Uid); exist {
			urlAndfilename := strings.SplitN(Rid.(string), ",", 2)
			HandleRetry(urlAndfilename[0], urlAndfilename[1], Uid, true)
		}
	}
}

func main() {
	go func() {
		file, _ := os.OpenFile("Log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		for msg := range ch {
			_, _ = file.WriteString(msg + "\n")
		}
		defer close(ch)
	}()
	HandleDownload()
}
