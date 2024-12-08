package wal

import (
	"fmt"
	"io"
	"os"
	"simplex"
	"sync"
)

var (
	_ simplex.WriteAheadLog = &WriteAheadLog{}
)

type WriteAheadLog struct {
	file *os.File

	// one writer multiple readers lock
	rwMutex sync.RWMutex

}

func New() (*WriteAheadLog, error) {
	filename := WalFilename + WalExtension
	file, err := os.OpenFile(filename, WalFlags, WalPermissions)
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		file: file,
		rwMutex: sync.RWMutex{},
	}, nil
}

// Appends a record to the write ahead log 
// Must flush the OS cache on every append to ensure consistency
func (w *WriteAheadLog) Append(r *simplex.Record) error {
	bytes := r.Bytes()
	
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()

	
	// write will append
	amount, err := w.file.Write(bytes)
	if err != nil {
		return err
	}
	fmt.Printf("Wrote %d bytes \n", amount)

	// ensure file gets written to SSD
	return w.file.Sync()
}

func (w *WriteAheadLog) ReadAll() ([]simplex.Record, error) {
	err := w.seekToStart()
	if err != nil {
		return []simplex.Record{}, fmt.Errorf("error seeking to start %w", err)
	}

	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()

	records := []simplex.Record{}
	for {
		var record simplex.Record
		n, err := record.FromBytes(w.file)
		fmt.Printf("num bytes read %d \n", n)

		// finished reading
		if err == io.EOF {
			break
		} else if err != nil {
			// if we error here, we need to ensure the next write will be at the end of the file
			return []simplex.Record{}, ErrReadingRecord
		}

		records = append(records, record)
	}

	// do we need to reset the os.File ptr?
	return records, nil
}

func (w *WriteAheadLog) Close() error {
	return w.file.Close()
}

// We can either ensure to seek to the start of the file before reading or writing,
// or have two file ptrs one for reading and another for writing
func (w *WriteAheadLog) seekToStart() error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()

	_, err := w.file.Seek(0, io.SeekStart)
	return err
}
