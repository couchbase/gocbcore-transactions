package transactions

// SerializedContext represents a transaction which has been serialized
// for resumption at a later point in time.
type SerializedContext struct {
}

// EncodeAsString will encode this SerializedContext to a string which
// can be decoded later to resume the transaction.
func (c *SerializedContext) EncodeAsString() string {
	return ""
}

// EncodeAsBytes will encode this SerializedContext to a set of bytes which
// can be decoded later to resume the transaction.
func (c *SerializedContext) EncodeAsBytes() []byte {
	return []byte{}
}
