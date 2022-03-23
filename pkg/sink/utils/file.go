/**
 * @Author: hexing
 * @Description:
 * @File:  file
 * @Version: 1.0.0
 * @Date: 20-6-6 上午9:53
 */

package utils

import (
	"os"
)

// 判断文件是否存在
func IsExist(fileAddr string) bool {
	// 读取文件信息，判断文件是否存在
	_, err := os.Stat(fileAddr)
	if err != nil {
		if os.IsExist(err) { // 根据错误类型进行判断
			return true
		}
		return false
	}
	return true
}
