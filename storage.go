package storage

import (
	"github.com/scottcagno/storage/pkg/lsmt/binary"
)

// Config is an interface for this package
type Config interface {
	CheckConfig() Config
}

// Storage is an interface for this package
type Storage interface {
	Has(key string) bool
	Put(key string, value []byte) error
	PutBatch(batch *binary.Batch) error
	Get(key string) ([]byte, error)
	GetBatch(keys ...string) (*binary.Batch, error)
	Del(key string) error
	Stats() (StorageStats, error)
	Close() error
}

// StorageStats is an interface for this package
type StorageStats interface {
	String() string
	JSON() (string, error)
}

// Examples of struct field tags and their meanings:
//
//   // Field appears in JSON as key "myName".
//   Field int `json:"myName"`
//
//   // Field appears in JSON as key "myName" and
//   // the field is omitted from the object if its value is empty,
//   // as defined above.
//   Field int `json:"myName,omitempty"`
//
//   // Field appears in JSON as key "Field" (the default), but
//   // the field is skipped if empty.
//   // Note the leading comma.
//   Field int `json:",omitempty"`
//
//   // Field is ignored by this package.
//   Field int `json:"-"`
//
//   // Field appears in JSON as key "-".
//   Field int `json:"-,"`
//
// The "string" option signals that a field is stored as JSON inside a
// JSON-encoded string. It applies only to fields of string, floating point,
// integer, or boolean types. This extra level of encoding is sometimes used
// when communicating with JavaScript programs:
//
//    Int64String int64 `json:",string"`
