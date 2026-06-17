package main

import (
	"context"
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
var ch chan string = make(chan string) // 日志通道，所有下载相关的日志都通过这个通道发送，主函数中有一个goroutine专门负责消费这个通道中的日志并写入文件
var IDs sync.Map = sync.Map{}          // 存储下载请求的ID和对应的URL和文件名，方便重试时查找
// var Retrychan chan string = make(chan string) // 重试通道，当下载失败时，将URL发送到这个通道
var MaxID int = 0                                  // 下载请求的ID自增变量，每次有新的下载请求时，MaxID加1，作为新请求的ID，方便记录和重试
var Limiter chan struct{} = make(chan struct{}, 5) // 下载并发限制器，容量为5，表示最多同时有5个下载任务在进行，超过这个数量的下载请求会被加入等待队列或者直接返回错误提示
var retryLimit sync.Map = sync.Map{}               // 存储每个URL的重试次数，避免同一个URL被频繁重试
var waitqueue chan Request = make(chan Request, 5) // 下载等待队列，容量为5，表示最多有5个下载请求在等待队列中等待下载，超过这个数量的下载请求会直接返回错误提示
var CacheRecord []Record = []Record{}              // 下载记录缓存，避免频繁访问数据库

// Record结构体表示一个下载记录，包含下载请求的ID、下载状态、下载URL和下载文件名，方便记录和查询下载历史
type Record struct {
	ID       int    `json:"ID"`       // 下载请求的ID，唯一标识一个下载请求，方便记录和重试
	Status   string `json:"Status"`   // 下载状态，表示下载请求当前的状态，例如"下载中..."、"下载成功"、"下载失败"等，方便记录和查询
	URL      string `json:"URL"`      // 下载请求的URL，表示下载请求的文件地址,方便记录和查询
	Filename string `json:"Filename"` // 下载请求的文件名，表示下载请求的文件名，方便记录和查询
}

// Request结构体表示一个下载请求，包含URL、文件名、请求ID和一个标志位，标志位用于区分是新下载请求还是重试请求，新下载请求flag为false，重试请求flag为true，方便在记录下载状态时区分是新下载还是重试下载
type Request struct {
	URL      string
	Filename string
	id       int
	flag     bool
}

func DownloadFile(url string, filename string, id int, flag bool) (err error) {
	HandleStatus("下载中...", id, flag, url, filename)
	var Done chan string = make(chan string) // 下载完成信号通道，当下载完成时，将"下载完成"发送到这个通道
	go func() {                              // 启动一个goroutine来处理下载完成后的逻辑，例如释放下载限制器、处理等待队列等，避免下载函数中的逻辑过于复杂，影响下载性能
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
	if count, exist := retryLimit.Load(url); exist { // 如果这个URL之前下载失败过，则从retryLimit中获取重试次数，如果重试次数大于等于3次，则直接返回错误提示，避免一直重试
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour) // 设置一个较长的超时时间，实际应用中应该根据需求调整
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err = http.DefaultClient.Do(req)
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
		id := MaxID              // 生成新的下载请求ID
		_, Iflag := IDs.Load(id) // 检查这个ID是否已经存在，如果存在说明这个ID之前有过下载请求，可能是重试请求，Iflag为true，否则为false
		select {
		case Limiter <- struct{}{}:
		default: // 如果下载限制器已满，则将下载请求加入等待队列，等待下载限制器有空闲时再开始下载
			mutex := &sync.Mutex{}
			mutex.Lock()
			if len(waitqueue) == 5 {
				HandleStatus("下载请求太多了请稍候再试", id, Iflag, url, filename)
				continue
			}
			waitqueue <- Request{url, filename, MaxID, false}
			HandleStatus("下载文件过多已将文件加入等待队列，稍后开始下载", id, Iflag, url, filename)
			mutex.Unlock()
			continue
		}
		go func() {
			IDs.Store(id, url+","+filename)
			HandleStatus("开始下载", id, Iflag, url, filename)
			Iflag = true // 标志位设为true，表示这个下载请求已经存在过了，方便在HandleStatus函数中更新下载状态
			_ = DownloadFile(url, filename, id, Iflag)
		}()
	}
}

// 初始化下载记录缓存，从文件中读取下载记录，并初始化下载请求ID的最大值和下载请求ID与URL和文件名的映射关系，方便后续的下载记录查询和重试功能
func init() {
	data, err := os.ReadFile("history.json")
	if err != nil {
		panic(err)
	}
	var records []Record
	json.Unmarshal(data, &records)
	CacheRecord = records // 初始化下载记录缓存
	if len(records) > 0 {
		MaxID = records[len(records)-1].ID
	}
	for _, record := range records {
		IDs.Store(record.ID, record.URL+","+record.Filename)
	}
}

// HandleStatus函数用于更新下载记录的状态，接收下载状态、下载请求ID、标志位、下载URL和下载文件名作为参数，根据标志位判断是新下载请求还是重试请求，如果是新下载请求则创建一个新的下载记录并添加到缓存中，如果是重试请求则更新对应的下载记录的状态，最后将更新后的下载记录缓存写入文件，方便后续查询和重试功能
func HandleStatus(status string, id int, flag bool, url string, filename string) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	var records []Record = CacheRecord
	if !flag { // 如果是新下载请求，则创建一个新的下载记录并添加到缓存中，flag为false表示新下载请求，flag为true表示重试请求
		records = append(records, Record{id, status, url, filename})
	} else {
		for i, record := range records {
			if record.ID == id { // 如果是重试请求，则更新下载记录的状态
				records[i].Status = status
				break
			}
		}
	}
	RWmutex := &sync.RWMutex{}
	RWmutex.Lock()
	CacheRecord = records
	RWmutex.Unlock()
}

func HandleRetry(url string, filename string, id int, flag bool) {
	Limiter <- struct{}{}
	go func() { // 使用goroutine进行重试，避免阻塞主线程
		fmt.Printf("开始重试下载文件:%s\n", filename)
		err := DownloadFile(url, filename, id, flag)
		fmt.Printf("重试结束!!!")
		if err != nil {
			HandleStatus("重试失败", id, flag, url, filename)
		}
	}()
}

func HandleList() {
	var records []Record = CacheRecord
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
	go func() { // 启动一个goroutine来处理日志通道中的日志，避免日志通道阻塞导致下载函数中的日志发送失败，影响下载状态的记录和查询功能
		file, _ := os.OpenFile("Log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		for msg := range ch {
			_, _ = file.WriteString(msg + "\n")
		}
		defer close(ch)
	}()
	// 启动下载处理函数，监听用户输入的下载请求，处理下载逻辑和状态记录逻辑
	HandleDownload()
	// 程序退出时将下载记录缓存写入文件
	data, err := json.MarshalIndent(CacheRecord, " ", " ")
	if err != nil {
		panic(err)
	}
	_ = os.WriteFile("history.json", data, 0644)
}
