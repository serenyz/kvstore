package kvstore

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// newRequestID 生成一个由 128 位加密随机数编码而成的十六进制请求标识。
// 返回值固定为 32 个小写字符，可安全地作为进程内 inflight 表的键。
func newRequestID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(data[:]), nil
}

// unwrapApplyResult 将可空的内部应用结果转换为值和错误。
// nil 通常意味着生产者违反了回执协议，因此会被转成明确错误，避免调用方解引用崩溃。
func unwrapApplyResult(result *applyResult) (record, error) {
	if result == nil {
		return record{}, errors.New("received nil apply result")
	}
	return result.record, result.err
}
