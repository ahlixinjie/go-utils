package set

type Set[KEY comparable] map[KEY]struct{}

func NewSet[KEY comparable](keys ...KEY) Set[KEY] {
	set := make(map[KEY]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return set
}

func (s Set[KEY]) Add(keys ...KEY) {
	for _, key := range keys {
		s[key] = struct{}{}
	}
}

func (s Set[KEY]) Del(keys ...KEY) {
	for _, key := range keys {
		delete(s, key)
	}
}

func (s Set[KEY]) Has(key KEY) bool {
	_, ok := s[key]
	return ok
}

func (s Set[KEY]) ToList() []KEY {
	result := make([]KEY, 0, len(s))

	for key := range s {
		result = append(result, key)
	}
	return result
}

//DifferenceSet return the element of s-a, and it won't change two set
func (s Set[KEY]) DifferenceSet(a Set[KEY]) []KEY {
	result := make([]KEY, 0, len(s)/2)

	for key := range s {
		if _, ok := a[key]; !ok {
			result = append(result, key)
		}
	}

	return result
}

//DifferenceKeys is almost same as DifferenceSet, but arguments is keys
func (s Set[KEY]) DifferenceKeys(keys ...KEY) []KEY {
	result := make([]KEY, 0, len(s)/2)

	for _, key := range keys {
		if _, ok := s[key]; !ok {
			result = append(result, key)
		}
	}
	return result
}
