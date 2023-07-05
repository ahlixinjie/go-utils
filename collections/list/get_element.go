package list

// GetFirstElement get the first element of the slice
// if the slice is empty, then return the default value
func GetFirstElement[T any](s []T) (result T) {
	if len(s) > 0 {
		return s[0]
	}
	return
}
