package main

import (
	"bufio"
	// "encoding/json"
	"os"
	"testing"
	"time"
)

// 添加缓存后第二个测试函数无法再依赖第一个测试函数的下载记录（因为不会修改history.json），
// 因此需要在第二个测试函数中重新初始化下载记录缓存,修改后第二个测试函数避免了对第一个测试函数的依赖，
// 增强了测试的独立性和可靠性，同时也避免了测试之间的状态干扰，确保每个测试函数都在一个干净的环境中运行，
// 提高了测试的准确性和稳定性，也避免了对文件系统的频繁读写，提升了测试的效率和性能
var RetryRecord []Record = []Record{
	{
		ID:       0,
		Status:   "文件下载成功,文件大小为14244字节,下载耗时0s，文件来源URL：https://picsum.photos/seed/guoxue1/400/300.jpg",
		URL:      "https://picsum.photos/seed/guoxue1/400/300.jpg",
		Filename: "test1",
	},
	{
		ID:       1,
		Status:   "下载失败",
		URL:      "xxx",
		Filename: "test2",
	},
}

// 并发读写文件的最佳实践是使用双句柄实现读写分离，而不是使用单句柄（因为需要频繁使用Seek()方法修改文件指针偏移量，还存在竞态条件）
func TestDownloadFile(t *testing.T) {
	ch = make(chan string)
	Limiter = make(chan struct{}, 5)
	CacheRecord = []Record{} // 清空下载记录缓存，确保测试环境干净
	// 1. 写入句柄（专用于日志记录）：追加模式，仅写入
	// 独立于读取句柄，避免写入偏移量影响读取指针
	go func() {
		file, _ := os.OpenFile("Log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		for msg := range ch {
			_, _ = file.WriteString(msg + "\n")
		}
		defer close(ch)
	}()
	err := os.Truncate("Log.log", 0) // 清空日志文件内容，确保测试环境干净
	if err != nil {
		t.Errorf("Error truncating file: %v", err)
	}
	// err = os.Truncate("history.json", 0) // 清空历史记录文件内容，确保测试环境干净
	// if err != nil {
	// 	t.Errorf("Error truncating file: %v", err)
	// }
	// 2. 读取句柄（专用于测试验证）：只读模式，从头开始
	// 由于操作系统为每个文件描述符独立维护偏移量
	// 此句柄不受上述后台写入协程的影响，始终从文件头读取完整内容
	file, err := os.Open("Log.log")
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}
	defer file.Close()
	Limiter <- struct{}{}
	HandleStatus("开始下载", 0, false, "https://picsum.photos/seed/guoxue1/400/300.jpg", "test1")
	_ = DownloadFile("https://picsum.photos/seed/guoxue1/400/300.jpg", "test1", 0, true)
	Limiter <- struct{}{}
	HandleStatus("开始下载", 1, false, "xxx", "test2")
	_ = DownloadFile("xxx", "test2", 1, true)
	// data, err := os.ReadFile("history.json")
	// if err != nil {
	// 	t.Errorf("Error reading file: %v", err)
	// }
	var Records []Record = CacheRecord
	var Logs []string
	// err = json.Unmarshal(data, &Records)
	// if err != nil {
	// 	t.Errorf("Error unmarshalling JSON: %v", err)
	// }
	for _, record := range Records {
		if record.ID == 0 {
			var status []rune = []rune(record.Status)
			if string(status[:6]) != "文件下载成功" {
				t.Errorf("Expected got '文件下载成功' but got %s", record.Status)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				Logs = append(Logs, scanner.Text()) // 获取当前行（不含换行符）
			}
			if Logs[record.ID] != "下载完成" {
				t.Errorf("Expected got '下载完成' but got %s", Logs[record.ID])
			}
		}
		if record.ID == 1 {
			var status []rune = []rune(record.Status)
			if string(status[:4]) != "下载失败" {
				t.Errorf("Expected got 下载失败 but got %s", record.Status)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				Logs = append(Logs, scanner.Text()) // 获取当前行（不含换行符）
			}
			if Logs[record.ID] != "Get \"xxx\": unsupported protocol scheme \"\"" {
				t.Errorf("Expected got 'Get \"xxx\": unsupported protocol scheme \"\"' but got %s", Logs[record.ID])
			}
		}
	}
}

func TestRetry(t *testing.T) {
	ch = make(chan string)
	Limiter = make(chan struct{}, 5)
	retryLimit.Store("xxx", 1) // 初始化重试次数，确保测试环境干净
	CacheRecord = []Record{}   // 清空下载记录缓存，确保测试环境干净
	go func() {
		file, _ := os.OpenFile("Log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // 以只写追加模式打开日志文件，避免之前的日志内容被覆盖
		defer file.Close()
		for msg := range ch {
			_, _ = file.WriteString(msg + "\n")
		}
		defer close(ch)
	}()
	CacheRecord = RetryRecord
	var Records []Record = CacheRecord
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second) // 确保重试状态更新完成
	if Records[1].Status != "重试失败" {
		t.Errorf("Expected got 重试失败 but got %s", Records[1].Status)
	}
	// 因为HandleRetry()是异步执行的，因此需要阻塞一段时间避免重试状态更新未完成或更新顺序错乱导致测试失败，实际测试时可以将重置时间改为5秒以加快测试速度
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	Records = CacheRecord
	if Records[1].Status != "下载失败，超过重试次数,请5分钟后重试" {
		t.Errorf("Expected got 下载失败，超过重试次数,请5分钟后重试 but got %s", Records[1].Status)
	}
	time.Sleep(5 * time.Second) // 确保重试次数重置完成
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	Records = CacheRecord
	if Records[1].Status != "重试失败" {
		t.Errorf("Expected got 重试失败 but got %s", Records[1].Status)
	}
}
