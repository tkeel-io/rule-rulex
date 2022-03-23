/**
 * @Author: hexing
 * @Description:
 * @File:  file_test.go
 * @Version: 1.0.0
 * @Date: 20-6-6 上午9:54
 */

package utils

import "testing"

func TestIsExist(t *testing.T) {
	type args struct {
		fileAddr string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
		{
			"test",
			args{fileAddr: "./cast.go"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsExist(tt.args.fileAddr); got != tt.want {
				t.Errorf("IsExist() = %v, want %v", got, tt.want)
			}
		})
	}
}
