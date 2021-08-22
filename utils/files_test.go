package utils

import (
	"testing"
)

func TestIsDir(t *testing.T) {
	if !IsDir("/") {
		t.Errorf("/ is dir")
	}

	if !IsDir("/tmp") {
		t.Errorf("/tmp is dir")
	}

	if IsDir("/dir_not_exists") {
		t.Errorf("/dir_not_exists is not dir")
	}

	if IsDir("/dev/hosts") {
		t.Errorf("/dev/hosts is not dir")
	}
}
