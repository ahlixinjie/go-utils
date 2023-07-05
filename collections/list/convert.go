package list

import "reflect"

type convertOption struct {
	ignoreZero bool
	params     []interface{}
}

type ConvertOption func(*convertOption)

func WithIgnoreZero() ConvertOption {
	return func(option *convertOption) {
		option.ignoreZero = true
	}
}

func WithParams(params ...interface{}) ConvertOption {
	return func(option *convertOption) {
		option.params = params
	}
}

// Converter will convert one type of slice to another type of slice.
func Converter[SOURCE any, DEST comparable](f func(source SOURCE, params ...interface{}) DEST, list []SOURCE, opts ...ConvertOption) (result []DEST) {
	var opt convertOption
	for _, o := range opts {
		o(&opt)
	}

	result = make([]DEST, 0, len(list))

	for _, v := range list {
		r := f(v, opt.params...)

		if opt.ignoreZero && reflect.ValueOf(r).IsZero() {
			continue
		}
		result = append(result, r)
	}
	return
}
