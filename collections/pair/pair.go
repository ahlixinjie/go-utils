package pair

type Pair[TYPE1, TYPE2 any] struct {
	first  TYPE1
	second TYPE2
}

func NewPair[TYPE1, TYPE2 any](first TYPE1, second TYPE2) *Pair[TYPE1, TYPE2] {
	return &Pair[TYPE1, TYPE2]{first: first, second: second}
}
func (p *Pair[TYPE1, TYPE2]) First() TYPE1 {
	return p.first
}
func (p *Pair[TYPE1, TYPE2]) Second() TYPE2 {
	return p.second
}
