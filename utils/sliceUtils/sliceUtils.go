package sliceUtils

func Map[T, U any](s []T, f func(T) U) []U {
	r := make([]U, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}

func Reverse[T any](s []T) []T {
	result := make([]T, len(s))
	for i := range s {
		result[i] = s[len(s)-1-i]
	}
	return result
}
