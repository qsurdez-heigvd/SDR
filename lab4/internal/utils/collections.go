package utils

// SliceContains reports whether a slice contains a given element.
func SliceContains[T comparable](slice []T, elem T) bool {
	for _, e := range slice {
		if e == elem {
			return true
		}
	}
	return false
}

// SliceContainsAll reports whether a slice contains all elements of another slice.
func SliceContainsAll[T comparable](slice []T, elems []T) bool {
	for _, e := range elems {
		if !SliceContains(slice, e) {
			return false
		}
	}
	return true
}

// SliceContainsSame reports whether two slices contain the same elements, regardless of order.
func SliceContainsSame[T comparable](slice1 []T, slice2 []T) bool {
	return SliceContainsAll(slice1, slice2) && SliceContainsAll(slice2, slice1)
}

// SliceEquals reports whether two slices are equal.
func SliceEquals[T comparable](slice1 []T, slice2 []T) bool {
	if len(slice1) != len(slice2) {
		return false
	}
	for i := range slice1 {
		if slice1[i] != slice2[i] {
			return false
		}
	}
	return true
}
