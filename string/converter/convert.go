package converter

import (
	"fmt"
	"strconv"
	"unicode"
)

func CamelCaseToUnderscore(s string) string {
	var output []rune

	for i, r := range s {
		if i == 0 {
			output = append(output, unicode.ToLower(r))
			continue
		}
		if unicode.IsUpper(r) {
			output = append(output, '_')
		}
		output = append(output, unicode.ToLower(r))
	}
	return string(output)
}

func MustAtoi(s string) int {
	if len(s) == 0 {
		return 0
	}
	result, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("err:%v, value:%s", err, s))
	}
	return result
}
