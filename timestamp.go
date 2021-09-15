package lease

import (
	"github.com/bcowtech/lib-redis-lease/internal"
)

func CurrentTimestamp() *Timestamp {
	return internal.CurrentTimestamp()
}
