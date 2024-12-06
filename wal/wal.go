package wal

import (
	"fmt"
	"os"
	"simplex"
	"sync"
)

var (
	_ simplex.WriteAheadLog = &WriteAheadLog{}
)

type WriteAheadLog struct {
	// one writer multiple readers lock
	rwMutex sync.RWMutex
}

// Appends a record to the write ahead log 
// Must flush the OS cache on every append to ensure consistency
func (w *WriteAheadLog) Append(r *simplex.Record) error {
	bytes := r.Bytes()
	
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()

	filename := filename + extension
	file, err := os.OpenFile(filename, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	defer file.Close()
	amount, err := file.Write(bytes)
	if err != nil {
		return err
	}
	fmt.Printf("Wrote %d bytes", amount)


	// TODO: flush file
	return nil
}

func (w *WriteAheadLog) ReadAll() ([]simplex.Record, error) {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()

	file, err := os.Open(filename + extension)
	if err != nil {
		return []simplex.Record{}, err
	}
	defer file.Close()

	var record simplex.Record
	n, err := record.FromBytes(file)
	fmt.Printf("num bytes read %d", n)
	if err != nil {
		return []simplex.Record{}, err
	}

	return []simplex.Record{record}, nil
}
