package utils

import ()

// 计算当前机器应该起多少个进程
// 先平均分配，剩下的按顺序分配
// 以serverCount=3为例
// processCount=1: 1, 0, 0
// processCount=2: 1, 1, 0
// processCount=3: 1, 1, 1
// processCount=4: 2, 1, 1
// processCount=5: 2, 2, 1
// processCount=6: 2, 2, 2
func CalcProcessCount(processCount int, serverCount int, serverIndex int) int {
	if serverIndex >= serverCount {
		return 0
	}

	// 只有一个服务
	if serverCount == 1 {
		return processCount
	}

	num := processCount / serverCount
	if serverIndex < processCount%serverCount {
		num += 1
	}

	return num
}
