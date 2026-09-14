package wal

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Entry struct {
	Operation string `json:"operation"`
	Key       int    `json:"key"`
	Value     string `json:"value,omitempty"`
}

type Log struct {
	file *os.File
	path string
	mu   sync.Mutex
}

func Open(path string) (*Log, error) {
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &Log{file: file, path: path}, nil
}

func (l *Log) Append(entry Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := json.NewEncoder(l.file).Encode(entry)
	if err != nil {
		return err
	}

	return l.file.Sync()
}

func (l *Log) ReadAll() ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	file, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []Entry
	decoder := json.NewDecoder(file)

	for {
		var entry Entry

		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.file.Close()
}
