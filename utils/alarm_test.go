package utils

import (
	"testing"
)

func TestAlarmSend(t *testing.T) {
	alarmSend("msgbus", "unit_test", "test")
}
