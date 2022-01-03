package omap

type OrderedMap[K comparable, V any] struct {
	data map[K]V
	keys []K
}
