package record

// Record is the atomic unit stored in timberdb.
type Record struct {
	Timestamp int64  // Unix nanoseconds — partition key
	SourceID  []byte // identifies the source stream
	Payload   []byte // opaque bytes; timberdb does not interpret these
}

// Iterator is the interface for scanning records in (Timestamp, SourceID) order.
type Iterator interface {
	Next() bool
	Record() Record
	Close() error
}
