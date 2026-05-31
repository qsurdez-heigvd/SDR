package ring

type ringIterator struct {
	addresses []address
	current   int
}

func newRingIterator(ring []address, self address) *ringIterator {
	selfIndex := -1
	for i, addr := range ring {
		if addr == self {
			selfIndex = i
			break
		}
	}

	if selfIndex == -1 {
		panic("ring iterator: self addr not found in ring")
	}

	return &ringIterator{
		addresses: ring,
		current:   selfIndex,
	}
}

func (ri *ringIterator) next() address {
	ri.current = (ri.current + 1) % len(ri.addresses)
	return ri.addresses[ri.current]
}

func (ri *ringIterator) value() address {
	return ri.addresses[ri.current]
}

func (ri *ringIterator) prev() address {
	ri.current = (ri.current - 1) % len(ri.addresses)
	return ri.addresses[ri.current]
}
