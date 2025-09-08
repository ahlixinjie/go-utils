package wrapper

import (
	"log/slog"
	"runtime/debug"
)

// WithRecover 返回一个有recover的函数，注意需要调用
func WithRecover(f func()) func() {
	return func() {
		defer func() {
			if err := recover(); err != nil {
				slog.Error("recover err:", err)
				slog.Error("stack:", string(debug.Stack()))
			}
		}()
		f()
	}
}
