package record

// Record is the atomic unit stored in timberdb.
type Record struct {
	Timestamp int64  // Unix nanoseconds — partition key
	SourceID  []byte // identifies the source stream
	Payload   []byte // opaque bytes; timberdb does not interpret these
}

// RecordView is a zero-copy view of a record. SourceID and Payload are slices into
// an underlying block buffer; they are valid only until the next call to Next().
// Call Clone to obtain an owned copy that outlives the iterator.
type RecordView struct {
	Timestamp int64
	SourceID  []byte
	Payload   []byte
}

// Clone returns a fully owned Record copied from the view.
func (v RecordView) Clone() Record {
	src := make([]byte, len(v.SourceID))
	copy(src, v.SourceID)
	p := make([]byte, len(v.Payload))
	copy(p, v.Payload)
	return Record{Timestamp: v.Timestamp, SourceID: src, Payload: p}
}

// Iterator is the interface for scanning records in (Timestamp, SourceID) order.
// After Next returns false, callers must check Err() to distinguish normal
// exhaustion from a read error.
//
// View returns a zero-copy view of the current record. The view's SourceID and
// Payload slices are valid only until the next call to Next. Call Record() or
// Clone() to obtain an owned copy.
type Iterator interface {
	Next() bool
	Record() Record
	View() RecordView
	Close() error
	Err() error
}
