package cache

import (
	"github.com/fxamacker/cbor/v2"
)

var ingressEncoder, _ = cbor.CoreDetEncOptions().EncMode()

func marshalBlindCBOR(v any) ([]byte, error) { return ingressEncoder.Marshal(v) }
