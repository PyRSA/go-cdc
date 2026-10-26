package client

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// BulkBatch is one bulk request: the operations it carries and the exact NDJSON
// body that is sent for them. The body is encoded once, before the request, so
// a byte budget checked by the caller is the byte count Elasticsearch receives.
type BulkBatch struct {
	Ops  []BulkOperation
	Body []byte
}

// EncodeBulkOperation returns the bytes op contributes to a bulk request: the
// action line plus, for index, the document line — both newline terminated, as
// the bulk API requires. The encoding is deterministic, so measuring it never
// disagrees with what is written to the socket.
func EncodeBulkOperation(op BulkOperation) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	header := map[string]map[string]string{
		string(op.Action): {"_index": op.Index, "_id": op.ID},
	}
	if err := enc.Encode(header); err != nil {
		return nil, fmt.Errorf("elasticsearch: bulk: encode header: %w", err)
	}
	switch op.Action {
	case ActionIndex:
		if len(op.Document) == 0 {
			return nil, fmt.Errorf("elasticsearch: bulk: index %q id %q has an empty document", op.Index, op.ID)
		}
		buf.Write(op.Document)
		buf.WriteByte('\n')
	case ActionDelete:
	default:
		return nil, fmt.Errorf("elasticsearch: bulk: unsupported action %q", op.Action)
	}
	return buf.Bytes(), nil
}

// EncodeBulk returns the body of one request carrying ops: exactly the
// concatenation of EncodeBulkOperation, which lets a caller size a request
// before it is built.
func EncodeBulk(ops []BulkOperation) ([]byte, error) {
	var body []byte
	for _, op := range ops {
		encoded, err := EncodeBulkOperation(op)
		if err != nil {
			return nil, err
		}
		body = append(body, encoded...)
	}
	return body, nil
}

// NewBulkBatch encodes ops into a single request. An empty input yields an
// empty batch, which the caller must not send.
func NewBulkBatch(ops []BulkOperation) (BulkBatch, error) {
	body, err := EncodeBulk(ops)
	if err != nil {
		return BulkBatch{}, err
	}
	return BulkBatch{Ops: ops, Body: body}, nil
}
