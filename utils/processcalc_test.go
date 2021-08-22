package utils

import (
	"fmt"
	"testing"
)

func TestCalcProcessCount(t *testing.T) {
	// 测试3台机器，0-6个进程的所有case
	if err := calcProcessCountAndCompare(0, 3, []int{0, 0, 0}); err != nil {
		t.Errorf(err.Error())
	}

	if err := calcProcessCountAndCompare(1, 3, []int{1, 0, 0}); err != nil {
		t.Errorf(err.Error())
	}

	if err := calcProcessCountAndCompare(2, 3, []int{1, 1, 0}); err != nil {
		t.Errorf(err.Error())
	}

	if err := calcProcessCountAndCompare(3, 3, []int{1, 1, 1}); err != nil {
		t.Errorf(err.Error())
	}

	if err := calcProcessCountAndCompare(4, 3, []int{2, 1, 1}); err != nil {
		t.Errorf(err.Error())
	}

	if err := calcProcessCountAndCompare(5, 3, []int{2, 2, 1}); err != nil {
		t.Errorf(err.Error())
	}

	if err := calcProcessCountAndCompare(6, 3, []int{2, 2, 2}); err != nil {
		t.Errorf(err.Error())
	}

	// 测试5台机器，64个进程
	if err := calcProcessCountAndCompare(64, 5, []int{13, 13, 13, 13, 12}); err != nil {
		t.Errorf(err.Error())
	}

	// 测试5台机器，22个进程
	if err := calcProcessCountAndCompare(22, 5, []int{5, 5, 4, 4, 4}); err != nil {
		t.Errorf(err.Error())
	}
}

func calcProcessCountAndCompare(processCount int, serverCount int, expect []int) error {
	if serverCount != len(expect) {
		return fmt.Errorf("serverCount!=expect. serverCount:%v, expect:%v", serverCount, len(expect))
	}

	for i := 0; i < serverCount; i++ {
		count := CalcProcessCount(processCount, serverCount, i)
		e := expect[i]
		if count != expect[i] {
			return fmt.Errorf("calc process count error. processCount:%v, serverCount:%v, i:%v, expect:%v, count:%v", processCount, serverCount, i, e, count)
		}
	}

	return nil
}
