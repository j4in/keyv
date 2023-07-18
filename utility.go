package keyv

// Check2 is a variant of Check, where two values are expected. It will return the value if error is empty
func Check2[T any](v T, err error) T {
	Check(err)
	return v
}

// Check will panic if error
func Check(err error) {
	if err != nil {
		panic(err)
	}
}
