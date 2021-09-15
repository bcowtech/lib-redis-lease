package internal

import (
	"encoding/json"
	"time"
)

// WARNING: DISABLE go generate!
//  The `go generate` cannot process the type time.Duration well. If you call
//  `go generate` again, the output file must use msgp.Reader.ReadInt64() or
//  msgp.Writer.WriteInt64() to process these fields of type time.Duration.
//_ go:generate msgp -tests=false

type Lease struct {
	ID       string        `json:"id"                   msg:"id"`
	TTL      time.Duration `json:"ttl"                  msg:"ttl"`
	ExpireAt *Timestamp    `json:"expire_at,omitempty"  msg:"expire_at"`
}

func (l *Lease) TimeToLive() *time.Duration {
	if l == nil {
		return nil
	}

	if l.ExpireAt != nil {
		expireAt := l.ExpireAt.ToTime()
		result := time.Now().Sub(expireAt)
		return &result
	}
	return nil
}

func (l *Lease) MarshalJSON() ([]byte, error) {
	type Alias Lease
	return json.Marshal(&struct {
		TTL int64 `json:"ttl"`
		*Alias
	}{
		TTL:   int64(l.TTL / time.Millisecond),
		Alias: (*Alias)(l),
	})
}

func (l *Lease) UnmarshalJSON(data []byte) error {
	type Alias Lease
	aux := &struct {
		TTL int64 `json:"ttl"`
		*Alias
	}{
		Alias: (*Alias)(l),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	l.TTL = time.Duration(aux.TTL * int64(time.Millisecond))
	return nil
}
