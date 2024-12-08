package wal

import "os"

const (
	// extension for the WAL
	WalExtension = ".wal"
	// filename
	WalFilename = "temp"
	// will truncate file if it exists(makes it easier for testing)
	WalFlags = os.O_APPEND | os.O_CREATE | os.O_RDWR | os.O_TRUNC
	WalPermissions = 0666
)