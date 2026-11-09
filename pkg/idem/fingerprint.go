package idem

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/gowebpki/jcs"
)

// ComputeFingerprint calculates a deterministic SHA-256 hash across HTTP method,
// path, and canonicalized body. If the body is valid JSON, object keys are sorted
// according to RFC 8785 (JCS) and redundant whitespace is eliminated.
func ComputeFingerprint(method, path string, body []byte) string {
	normMethod := strings.ToUpper(strings.TrimSpace(method))
	normPath := strings.TrimSpace(path)

	canonicalBody := canonicalizeBody(body)

	h := sha256.New()
	h.Write([]byte(normMethod))
	h.Write([]byte("\n"))
	h.Write([]byte(normPath))
	h.Write([]byte("\n"))
	h.Write(canonicalBody)

	return hex.EncodeToString(h.Sum(nil))
}

// canonicalizeBody formats JSON using RFC 8785 canonicalization (JCS).
// If the body is empty or not valid JSON, it returns the trimmed bytes.
func canonicalizeBody(body []byte) []byte {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return []byte{}
	}
	canonical, err := jcs.Transform(trimmed)
	if err != nil {
		return trimmed
	}
	return canonical
}
