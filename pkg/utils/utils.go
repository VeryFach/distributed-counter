package utils

import (
	"crypto/rand"
	"encoding/hex"
)

func GenerateID(length int) string {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}