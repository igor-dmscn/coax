//go:build race

package ws

// raceEnabled reports whether the binary was built with -race. The race
// runtime allocates on its own, so allocation assertions cannot be measured
// under it.
const raceEnabled = true
