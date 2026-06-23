package crypto

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
)

// NewRandomSource returns a *rand.Rand suitable for use as the RandomSource
// field in simplex.EpochConfig
func NewRandomSource() *rand.Rand {
	var b [16]byte
	var seed1, seed2 uint64
	if _, err := cryptorand.Read(b[:]); err == nil {
		seed1 = binary.LittleEndian.Uint64(b[:8])
		seed2 = binary.LittleEndian.Uint64(b[8:])
	} else {
		// Fallback to fixed seeds (acceptable for testing contexts only)
		seed1 = 0xdeadbeefcafebabe
		seed2 = 0x0123456789abcdef
	}
	return rand.New(rand.NewPCG(seed1, seed2))
}
