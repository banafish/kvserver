package util

import (
	"github.com/google/uuid"
)

func GenerateClientID() string {
	return "cl_" + uuid.NewString()[:5]
}
