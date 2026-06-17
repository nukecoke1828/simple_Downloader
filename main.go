package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"os"
	"strconv"
	"time"
)

var defaultDownloadPath = "C:\\Users\\imdxh\\Downloads"  // 默认下载路径
var ch chan string = make(chan string)                   // 日志通道
var bucket chan interface{} = make(chan interface{}, 5)  // 限流器
var retryLimit sync.Map                                  // 重试次数记录（使用sync.Map避免并发问题）
var retrych chan string = make(chan string)              // 重试通道
var waitdownloadCh chan Request = make(chan Request, 10) // 等待下载队列

// 请求结构体，包含响应和请求对象
type Request struct {
	resp http.ResponseWriter
	req  *http.Request
}

func DownloadFile(url string, ch chan<- string, filename string, nowTime time.Time) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour) // 设置一个较长的超时时间，实际应用中应该根据需求调整
	defer cancel()
	// 废弃的重试与超时机制
	// var Done chan string = make(chan string)
	// go func() {
	// 	ticker := time.NewTicker(10 * time.Second)
	// 	timer := time.NewTimer(5 * time.Hour) // 设置一个较长的超时时间，实际应用中应该根据需求调整,这里设置为5小时，避免过早超时
	// 	defer timer.Stop()
	// 	defer ticker.Stop()
	// 	for {
	// 		select {
	// 		case result := <-Done:
	// 			if (result == "文件下载成功") || (result == "下载文件失败") {
	// 				if result == "下载文件失败" {
	// 					if count, exists := retryLimit.Load(filename); !exists || (exists && count.(int) < 3) {
	// 						retrych <- url + "," + filename // 将需要重试的URL和文件名发送到重试通道
	// 						go func(count int) {
	// 							time.Sleep(time.Second * 1)
	// 							if count_nuevo, _ := retryLimit.Load(filename); count_nuevo == count {
	// 								<-retrych                               // 用户30秒内没有重试，自动放弃重试机会
	// 								timer := time.NewTimer(5 * time.Minute) // 过段时间再重置重试次数避免用户短时间内重试过多次
	// 								defer timer.Stop()
	// 								<-timer.C
	// 								retryLimit.Delete(filename) // 放弃重试机会，重置重试次数记录
	// 							}
	// 						}(count.(int))
	// 					} else {
	// 						go func() {
	// 							timer := time.NewTimer(5 * time.Minute) // 过段时间再重置重试次数避免用户短时间内重试过多次
	// 							defer timer.Stop()
	// 							<-timer.C
	// 							retryLimit.Delete(filename) // 重试次数超过限制，重置重试次数记录
	// 						}()
	// 					}
	// 				}
	// 				go func() {
	// 					timer := time.NewTimer(5 * time.Second) // 稍微等待一下确保限流器已经释放桶位
	// 					defer timer.Stop()
	// 					<-timer.C
	// 					select {
	// 					case request := <-waitdownloadCh: // 如果有等待下载的请求，尝试处理下一个下载请求
	// 						q := request.req.URL.Query()
	// 						q.Add("wait", "yes")                  // 添加参数
	// 						request.req.URL.RawQuery = q.Encode() // 重新编码回 RawQuery
	// 						ch <- "已处理等待队列中的下载请求"
	// 						DownloadFileHandler().ServeHTTP(request.resp, request.req)
	// 					default:
	// 						ch <- "当前没有等待下载的请求"
	// 					}
	// 				}()
	// 				return
	// 			}
	// 		case <-ticker.C:
	// 			ch <- "文件较大或网络较慢，下载仍在进行中..."
	// 		case <-timer.C:
	// 			ch <- "下载文件超时，可能网络较慢或文件过大，请稍后再试"
	// 			Done <- "下载文件失败"
	// 			return
	// 		}
	// 	}
	// }()
	Download := func() chan error { // 使用匿名函数闭包获取变量，便于进行超时控制
		errCh := make(chan error, 1)
		if count, exists := retryLimit.Load(filename); exists { // 如果重试次数记录存在，则增加重试次数
			count = count.(int) + 1
			retryLimit.Store(filename, count)
			if count.(int) > 3 { // 如果重试次数超过限制，则返回错误
				ch <- "下载文件失败，重试次数超过限制"
				// Done <- "下载文件失败"
				errCh <- errors.New("下载文件失败，重试次数超过限制")
				return errCh
			}
		} else { // 如果重试次数记录不存在，则初始化为1
			retryLimit.Store(filename, 1)
		}
		// 发送带有超时的请求
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := http.DefaultClient.Do(req) // 不使用http.Get()，因为http.Get()没有提供设置请求超时的选项，使用http.DefaultClient.Do()可以配合context实现请求超时控制
		if err != nil {
			ch <- "下载文件失败" + err.Error()
			go RetryDownload(filename, url)
			// Done <- "下载文件失败"
			errCh <- err
			return errCh
		}
		defer resp.Body.Close()
		// 创建文件
		// 使用os.Create()而不是os.WriteFile()是因为后续需要使用*os.File文件句柄进行流式写入，避免占用过多内存，尤其是对于较大的文件
		data, err := os.Create(defaultDownloadPath + "\\" + filename)
		if err != nil {
			ch <- "下载文件失败" + err.Error()
			go RetryDownload(filename, url)
			// Done <- "下载文件失败"
			errCh <- err
			return errCh
		}
		defer data.Close()
		// 写入文件（对于较大的文件应该使用io.Copy对于较小的文件可以使用io.ReadAll,因为io.Copy是流式写入不会占用过多内存）
		_, err = io.Copy(data, resp.Body)
		if err != nil {
			ch <- "下载文件失败" + err.Error()
			go RetryDownload(filename, url)
			// Done <- "下载文件失败"
			errCh <- err
			return errCh
		}
		// 获取文件大小
		// os.Stat()函数返回一个描述文件元信息的os.FileInfo结构体，其中Size()方法返回文件大小
		length, err := os.Stat(defaultDownloadPath + "\\" + filename)
		if err != nil {
			ch <- "获取文件大小失败" + err.Error()
			go RetryDownload(filename, url)
			// Done <- "下载文件失败"
			errCh <- err
			return errCh
		}
		ch <- "文件下载成功" + ",文件大小为" + strconv.Itoa(int(length.Size())) + "字节,下载耗时" + time.Since(nowTime).String() + "，文件来源URL：" + url
		go func() { // 检查等待队列，如果有等待下载的请求，尝试处理下一个下载请求
			timer := time.NewTimer(5 * time.Second) // 稍微等待一下确保限流器已经释放桶位
			defer timer.Stop()
			<-timer.C
			select {
			case request := <-waitdownloadCh: // 如果有等待下载的请求，尝试处理下一个下载请求
				q := request.req.URL.Query()
				q.Add("wait", "yes")                  // 添加参数
				request.req.URL.RawQuery = q.Encode() // 重新编码回 RawQuery
				ch <- "已处理等待队列中的下载请求"
				DownloadFileHandler().ServeHTTP(request.resp, request.req)
			default:
				ch <- "当前没有等待下载的请求"
			}
		}()
		// Done <- "文件下载成功"
		errCh <- nil
		return errCh
	}
	ticker := time.NewTicker(10 * time.Second)
	go func() { // 提示用户下载还在继续
		<-ticker.C
		ticker.Stop()
		ch <- "文件较大或网络较慢，下载仍在进行中..."
	}()
	defer ticker.Stop()
	select { // 等待下载完成或超时
	case <-ctx.Done():
		ch <- "下载文件超时，可能网络较慢或文件过大，请稍后再试"
		// Done <- "下载文件失败"
		return errors.New("下载文件超时，可能网络较慢或文件过大，请稍后再试")
	case err := <-Download():
		if err != nil {
			return err
		}
		return nil
	}
}

func RetryDownload(filename string, url string) {
	if count, exists := retryLimit.Load(filename); !exists || (exists && count.(int) < 3) {
		retrych <- url + "," + filename // 将需要重试的URL和文件名发送到重试通道
		go func(count int) {
			time.Sleep(time.Second * 1) // 阻塞等待用户发送重试请求
			if count_nuevo, _ := retryLimit.Load(filename); count_nuevo == count {
				<-retrych                               // 用户30秒内没有重试，自动放弃重试机会
				timer := time.NewTimer(5 * time.Minute) // 过段时间再重置重试次数避免用户短时间内重试过多次
				defer timer.Stop()
				<-timer.C
				retryLimit.Delete(filename) // 放弃重试机会，重置重试次数记录
			}
		}(count.(int))
	} else {
		go func() {
			timer := time.NewTimer(5 * time.Minute) // 过段时间再重置重试次数避免用户短时间内重试过多次
			defer timer.Stop()
			<-timer.C
			retryLimit.Delete(filename) // 重试次数超过限制，重置重试次数记录
		}()
	}
}

func DownloadFileHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.Query().Get("url")
		filename := r.URL.Query().Get("filename")
		wait := r.URL.Query().Get("wait") // 避免将等待中的请求加入等待队列
		if url == "" {
			http.Error(w, "URL参数不能为空", http.StatusBadRequest)
			return
		}
		if filename == "" {
			filename = "downloaded_file_" + strconv.FormatInt(time.Now().Unix(), 10) // 如果没有提供文件名，使用默认命名方式
		}
		nowTime := time.Now()
		mutex := sync.Mutex{} // 避免多个goroutine同时访问等待队列导致的并发问题，使用互斥锁确保同一时间只有一个goroutine能够访问等待队列
		mutex.Lock()
		if len(waitdownloadCh) != 0 && wait != "yes" { // 优先处理等待下载的请求，当前有等待下载的请求时，新的下载请求直接加入等待队列
			if len(waitdownloadCh) == 10 {
				http.Error(w, "下载请求过多，请稍后再试", http.StatusTooManyRequests)
				return
			}
			lock := sync.Mutex{}
			lock.Lock()
			waitdownloadCh <- Request{resp: w, req: r} // 将请求加入等待队列
			w.Write([]byte("当前有等待下载的请求，已将您的请求加入等待队列，稍后开始下载"))
			lock.Unlock()
			return
		}
		mutex.Unlock()
		select {
		case bucket <- struct{}{}: // 尝试占用一个桶位，限制并发数量
		default: // 如果桶位已满，则将请求加入等待队列
			if len(waitdownloadCh) == 10 {
				http.Error(w, "下载请求过多，请稍后再试", http.StatusTooManyRequests)
				return
			}
			lock := sync.Mutex{}
			lock.Lock()
			waitdownloadCh <- Request{resp: w, req: r} // 将请求加入等待队列
			w.Write([]byte("下载请求过多，已将您的请求加入等待队列，稍后开始下载"))
			lock.Unlock()
			return
		}
		err := DownloadFile(url, ch, filename, nowTime)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			<-bucket
		} else {
			w.Write([]byte("文件下载成功"))
			w.WriteHeader(http.StatusOK)
			<-bucket
		}
	}
}

// 用于处理重试下载的请求，用户在收到下载失败的提示后可以选择是否重试，如果选择重试则从重试通道接收重试信息并执行下载，如果选择放弃则直接返回错误提示，并且过段时间再重置重试次数记录避免用户短时间内重试过多次，使用方法用户在收到下载失败的提示后访问/retry接口并携带参数option=no表示放弃重试，携带参数option=yes表示重试，具体使用代码不做演示
func RetryDownloadFileHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		option := r.URL.Query().Get("option")
		msg := <-retrych // 从重试通道接收重试信息
		mutex := sync.Mutex{}
		mutex.Lock()                // 避免一个通道被多个goroutine同时关闭导致的panic，使用互斥锁确保同一时间只有一个goroutine能够关闭通道
		close(retrych)              // 关闭重试通道避免goroutine阻塞
		retrych = nil               // 将重试通道置为nil，避免重复关闭
		retrych = make(chan string) // 重新创建一个重试通道
		mutex.Unlock()
		parts := strings.SplitN(msg, ",", 2)
		if len(parts) != 2 {
			http.Error(w, errors.New("无效的重试信息").Error(), http.StatusBadRequest)
			return
		}
		url := parts[0]
		filename := parts[1]
		nowTime := time.Now()
		if option == "no" {
			go func() {
				timer := time.NewTimer(5 * time.Minute) // 过段时间再重置重试次数避免用户短时间内重试过多次
				defer timer.Stop()
				<-timer.C
				retryLimit.Delete(filename) // 放弃重试机会，重置重试次数记录
			}()
			http.Error(w, errors.New("用户放弃下载").Error(), http.StatusInternalServerError)
			return
		}
		bucket <- struct{}{} // 占用一个桶位，限制并发数量
		err := DownloadFile(url, ch, filename, nowTime)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			<-bucket
		} else {
			w.Write([]byte("文件下载成功"))
			w.WriteHeader(http.StatusOK)
			<-bucket
		}
	}
}

// 用于在尝试超过重试次数限制后重置全部重试次数记录，用于用户如果在5分钟内都没有执行任何下载操作，则自动重置重试次数记录，
// 允许用户重新尝试下载，使用方法使用goroutine监听下载通道，如果5分钟内没有收到下载请求，则自动重置重试次数记录，具体使用代码不做演示
func ResetRetryLimit(dlownloadCh <-chan string) {
	// 创建一个5分钟的定时器
	timer := time.NewTimer(5 * time.Minute)
	// 确保函数退出时停止定时器
	defer timer.Stop()
	// 无限循环，持续监听定时器和下载通道
	for {
		select {
		// 定时器触发的情况
		case <-timer.C:
			retryLimit = sync.Map{} // 重置重试次数记录
			log.Printf("重试次数记录已重置，用户可以重新尝试下载")
			timer.Reset(5 * time.Minute) // 重置定时器，继续监听下一轮
		case <-dlownloadCh: // 收到下载请求，重置定时器，继续监听下一轮
			timer.Reset(5 * time.Minute)
		}
	}
}

func main() {
	// 注册路由处理函数
	http.HandleFunc("/download", DownloadFileHandler())
	http.HandleFunc("/retry", RetryDownloadFileHandler())
	// 消费日志通道
	go func() {
		for msg := range ch {
			log.Printf("日志: %s", msg)
		}
		defer close(ch)
	}()
	// 启动HTTP服务器
	http.ListenAndServe(":8080", nil)
}
