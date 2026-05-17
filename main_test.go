package main

import (
	"log"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
)

var TestURL []string = []string{
	"https://picsum.photos/seed/guoxue1/400/300.jpg&filename=testphoto1.jpg",
	"https://picsum.photos/seed/zhonghua2/400/300.jpg&filename=testphoto2.jpg",
	"https://picsum.photos/seed/poet3/400/300.jpg&filename=testphoto3.jpg",
	"https://picsum.photos/seed/happiness4/400/300.jpg&filename=testphoto4.jpg",
	"https://picsum.photos/seed/western5/400/300.jpg&filename=testphoto5.jpg",
	"https://picsum.photos/seed/compare6/400/300.jpg&filename=testphoto6.jpg",
	"https://picsum.photos/seed/poetry7/400/300.jpg&filename=testphoto7.jpg",
	"fakeurl",
	"https://picsum.photos/seed/guoxue1/400/300.jpg&filename=testphoto1.jpg",
	"https://picsum.photos/seed/guoxue1/400/300.jpg&filename=testphoto1.jpg",
	"https://picsum.photos/seed/guoxue1/400/300.jpg&filename=testphoto1.jpg",
	"https://picsum.photos/seed/guoxue1/400/300.jpg&filename=testphoto1.jpg",
}

var TestReq []Req = []Req{
	{"https://picsum.photos/seed/guoxue1/400/300.jpg,testphoto1.jpg", "yes"},
	{"https://picsum.photos/seed/guoxue1/400/300.jpg,testphoto1.jpg", "no"},
	{"wrongurltestphoto1.jpg", "yes"},
	{"retry,fail", "yes"},
}

type Req struct {
	req    string
	status string
}

func TestDownloadFileHandler(t *testing.T) {
	ch = make(chan string)
	bucket = make(chan interface{}, 5)
	retrych = make(chan string)
	waitdownloadCh = make(chan Request, 10)
	go func() { // 消化日志通道中的日志，避免阻塞导致测试流程卡死
		for msg := range ch {
			log.Printf("日志: %s", msg)
		}
		defer close(ch)
	}()
	for idx, url := range TestURL {
		resp := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/download?url="+url, nil)
		if idx == 5 {
			for i := 0; i < 5; i++ {
				bucket <- struct{}{} // 模拟下载队列已满
			}
		}
		if idx == 7 {
			// 恢复下载队列，以便测试下一个请求
			bucket = make(chan interface{}, 5)
			waitdownloadCh = make(chan Request, 10)
		}
		DownloadFileHandler().ServeHTTP(resp, req)
		if idx < 5 {
			if resp.Code != 200 || resp.Body.String() != "文件下载成功" {
				t.Errorf("DownloadFileHandler failed to download file %s", url)
			}
			_, err := os.ReadFile(defaultDownloadPath + "\\testphoto" + strconv.Itoa(idx+1) + ".jpg") // 检查文件是否生成
			if err != nil {
				t.Errorf("DownloadFileHandler failed to find downloaded file %s", url)
			}
		}
		if idx == 5 {
			if resp.Body.String() != "下载请求过多，已将您的请求加入等待队列，稍后开始下载" {
				t.Errorf("Expected got 下载请求过多，已将您的请求加入等待队列，稍后开始下载 but got %s", resp.Body.String())
			}
		}
		if idx == 6 {
			if resp.Body.String() != "当前有等待下载的请求，已将您的请求加入等待队列，稍后开始下载" {
				t.Errorf("Expected got 当前有等待下载的请求，已将您的请求加入等待队列，稍后开始下载 but got %s", resp.Body.String())
			}
		}
		if idx == 7 {
			if resp.Body.String() != "Get \"fakeurl\": unsupported protocol scheme \"\"\n" {
				t.Errorf("Expected got Get \"fakeurl\": unsupported protocol scheme \"\"\n but got %s", resp.Body.String())
			}
			retryLimit = sync.Map{} // 重置重试次数记录，避免影响后续测试
		}
		if idx == 11 {
			if resp.Body.String() != "下载文件失败，重试次数超过限制\n" || resp.Code != 500 {
				t.Errorf("Expected got 下载文件失败，重试次数超过限制\n but got %s", resp.Body.String())
			}
		}
	}
}

func TestRetryDownloadFileHandler(t *testing.T) {
	ch = make(chan string)
	bucket = make(chan interface{}, 5)
	retrych = make(chan string)
	go func() {
		for msg := range ch {
			log.Printf("日志: %s", msg)
		}
		defer close(ch)
	}()
	for idx, req := range TestReq {
		go func() { // 异步发送避免阻塞测试流程
			retrych <- req.req // 模拟一个需要重试的下载请求
		}()
		resp := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/retry?option="+req.status, nil)
		RetryDownloadFileHandler().ServeHTTP(resp, req)
		if idx == 0 && (resp.Body.String() != "文件下载成功" || resp.Code != 200) {
			t.Errorf("Expected got 文件下载成功 but got %s", resp.Body.String())
		}
		if idx == 1 && (resp.Body.String() != "用户放弃下载\n" || resp.Code != 500) {
			t.Errorf("Expected got 用户放弃下载\n but got %s", resp.Body.String())
		}
		if idx == 2 && (resp.Body.String() != "无效的重试信息\n" || resp.Code != 400) {
			t.Errorf("Expected got 无效的重试信息\n but got %s", resp.Body.String())
		}
		if idx == 3 && (resp.Body.String() != "Get \"retry\": unsupported protocol scheme \"\"\n" || resp.Code != 500) {
			t.Errorf("Expected got Get \"retry\": unsupported protocol scheme \"\"\n but got %s", resp.Body.String())
		}
	}
}
