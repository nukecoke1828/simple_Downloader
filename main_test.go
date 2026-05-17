package main

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestDownloadFile(t *testing.T) {
	ch = make(chan string)
	Limiter = make(chan struct{}, 5)
	go func() {
		file, _ := os.OpenFile("Log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		for msg := range ch {
			_, _ = file.WriteString(msg + "\n")
		}
		defer close(ch)
	}()
	err := os.Truncate("Log.log", 0)
	if err != nil {
		t.Errorf("Error truncating file: %v", err)
	}
	err = os.Truncate("history.json", 0)
	if err != nil {
		t.Errorf("Error truncating file: %v", err)
	}
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
	data, err := os.ReadFile("history.json")
	if err != nil {
		t.Errorf("Error reading file: %v", err)
	}
	var Records []Record
	var Logs []string
	err = json.Unmarshal(data, &Records)
	if err != nil {
		t.Errorf("Error unmarshalling JSON: %v", err)
	}
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
	go func() {
		file, _ := os.OpenFile("Log.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		for msg := range ch {
			_, _ = file.WriteString(msg + "\n")
		}
		defer close(ch)
	}()

	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(5 * time.Second)
	data, err := os.ReadFile("history.json")
	if err != nil {
		t.Errorf("Error reading file: %v", err)
	}
	var Records []Record
	err = json.Unmarshal(data, &Records)
	if err != nil {
		t.Errorf("Error unmarshalling JSON: %v", err)
	}
	if Records[1].Status != "重试失败" {
		t.Errorf("Expected got 重试失败 but got %s", Records[1].Status)
	}
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	data, err = os.ReadFile("history.json")
	if err != nil {
		t.Errorf("Error reading file: %v", err)
	}
	err = json.Unmarshal(data, &Records)
	if err != nil {
		t.Errorf("Error unmarshalling JSON: %v", err)
	}
	if Records[1].Status != "下载失败，超过重试次数,请5分钟后重试" {
		t.Errorf("Expected got 下载失败，超过重试次数,请5分钟后重试 but got %s", Records[1].Status)
	}
	time.Sleep(5 * time.Second) // 需要测试时要将重置时间改为5秒
	HandleRetry("xxx", "test2", 1, true)
	time.Sleep(2 * time.Second)
	data, err = os.ReadFile("history.json")
	if err != nil {
		t.Errorf("Error reading file: %v", err)
	}
	err = json.Unmarshal(data, &Records)
	if err != nil {
		t.Errorf("Error unmarshalling JSON: %v", err)
	}
	if Records[1].Status != "重试失败" {
		t.Errorf("Expected got 重试失败 but got %s", Records[1].Status)
	}
}
