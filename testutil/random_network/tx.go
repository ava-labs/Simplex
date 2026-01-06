package random_network

import (
	"context"
	"crypto/rand"
	"encoding/asn1"
	"errors"
)

var errTxVerification = errors.New("tx verification failed")

type txID [32]byte

type TX struct {
	ID                     txID
	shouldFailVerification bool
}

type asn1TX struct {
	ID []byte
}

func (aTX asn1TX) toTX() *TX {
	var idArr txID
	copy(idArr[:], aTX.ID)
	return &TX{ID: idArr}
}

func CreateNewTX() *TX {
	id := make([]byte, 32)
	_, err := rand.Read(id)
	if err != nil {
		panic(err)
	}

	var idArr txID
	copy(idArr[:], id)

	return &TX{ID: idArr}
}

func (t *TX) SetShouldFailVerification(shouldFail bool) {
	t.shouldFailVerification = shouldFail
}

func (t *TX) Bytes() ([]byte, error) {
	return asn1.Marshal(asn1TX{ID: t.ID[:]})
}

func TxFromBytes(b []byte) (*TX, error) {
	var asn1TX asn1TX
	_, err := asn1.Unmarshal(b, &asn1TX)
	if err != nil {
		return nil, err
	}

	return asn1TX.toTX(), nil
}

func (t *TX) Verify(ctx context.Context) error {
	// TBD
	// Can set artificial failure here for testing or longer verification times
	if t.shouldFailVerification {
		return errTxVerification
	}
	return nil
}
