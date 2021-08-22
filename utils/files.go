package utils

import (
	"os"
)

// 是否是目录
func IsDir(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false
	}

	if !stat.IsDir() {
		return false
	}

	return true
}
