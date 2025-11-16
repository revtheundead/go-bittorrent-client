package bencode

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Decode is the high-level entry point for decoding a single
// bencoded value from the given string.
//
// It currently supports:
//   - strings: "<len>:<data>"
//   - integers: "i<integer>e"
//   - lists: "l<value1><value2>...e"
func Decode(s string) (interface{}, error) {
	b := []byte(s)
	if len(b) == 0 {
		return nil, errors.New("empty input")
	}

	// Decode a single value from the beginning of b.
	value, consumed, err := decodeValue(b)
	if err != nil {
		return nil, err
	}

	// Complain if there's junk after a valid value.
	if consumed != len(b) {
		return nil, fmt.Errorf("trailing data after valid bencode at byte %d", consumed)
	}

	return value, nil
}

// Encode is the high-level entry point for encoding a single
// Go value as bencode.
//
// Supported Go types:
//   - string, []byte
//   - int, int64
//   - []interface{}
//   - map[string]interface{}
func Encode(v interface{}) (string, error) {
	var buf bytes.Buffer

	if err := encodeValue(&buf, v); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// decodeValue decodes a single bencoded value starting at the
// beginning of b, and returns (value, bytesConsumed, error).
func decodeValue(b []byte) (interface{}, int, error) {
	if len(b) == 0 {
		return nil, 0, errors.New("unexpected end of input")
	}

	switch {
	case b[0] >= '0' && b[0] <= '9':
		// String: <length>:<data>
		return decodeBencodedString(b)
	case b[0] == 'i':
		// Integer: i<integer>e
		return decodeBencodedInteger(b)
	case b[0] == 'l':
		// List: l<value1><value2>...e
		return decodeBencodedList(b)
	case b[0] == 'd':
		// Dictionary: d<key1><value1>...e
		return decodeBencodedDict(b)
	default:
		return nil, 0, fmt.Errorf("unsupported bencode type %q", b[0])
	}
}

// encodeValue encodes a single Go value into the buffer as bencode.
func encodeValue(buf *bytes.Buffer, v interface{}) error {
	switch x := v.(type) {
	case string:
		return bencodeBytes(buf, []byte(x))
	case []byte:
		return bencodeBytes(buf, x)
	case int:
		return bencodeInteger(buf, int64(x))
	case int64:
		return bencodeInteger(buf, x)
	case []interface{}:
		return bencodeList(buf, x)
	case map[string]interface{}:
		return bencodeDict(buf, x)
	default:
		return fmt.Errorf("unsupported type for bencode encoding: %T", v)
	}
}

// -------------------- STRING --------------------

// decodeBencodedString decodes a bencoded string: "<length>:<data>".
// It returns the decoded string and the total number of bytes consumed.
func decodeBencodedString(b []byte) (string, int, error) {
	colonIndex := bytes.IndexByte(b, ':')
	if colonIndex == -1 {
		return "", 0, errors.New("invalid bencoded string: missing ':'")
	}

	lengthStr := string(b[:colonIndex])
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid string length %q: %w", lengthStr, err)
	}
	if length < 0 {
		return "", 0, fmt.Errorf("negative string length %d", length)
	}

	start := colonIndex + 1
	end := start + length

	if end > len(b) {
		return "", 0, fmt.Errorf("string length %d exceeds available data", length)
	}

	// Number of bytes consumed is everything up to the end of the string.
	consumed := end

	return string(b[start:end]), consumed, nil
}

// bencodeBytes encodes a []byte as "<length>:<data>".
func bencodeBytes(buf *bytes.Buffer, b []byte) error {
	buf.WriteString(strconv.Itoa(len(b)))
	buf.WriteByte(':')
	buf.Write(b)
	return nil
}

// -------------------- INTEGER --------------------

// decodeBencodedInteger decodes a bencoded integer: "i<data>e".
// It returns the int64 value and the total number of bytes consumed.
func decodeBencodedInteger(b []byte) (int64, int, error) {
	if len(b) < 3 || b[0] != 'i' {
		// Minimum valid integer is "i0e" (len 3)
		return 0, 0, errors.New("invalid bencoded integer: must start with 'i' and be at least 3 bytes")
	}

	eIndex := bytes.IndexByte(b, 'e')
	if eIndex == -1 {
		return 0, 0, errors.New("invalid bencoded integer: missing 'e' terminator")
	}
	if eIndex == 1 {
		// "ie" → empty value
		return 0, 0, errors.New("invalid bencoded integer: empty value")
	}

	valueString := string(b[1:eIndex])

	// Disallow leading zeros: "i01e" is invalid, but "i0e" is allowed.
	if len(valueString) > 1 && valueString[0] == '0' {
		return 0, 0, errors.New("invalid bencoded integer: leading zero")
	}

	// Disallow negative zero: "i-0e" is invalid.
	if strings.HasPrefix(valueString, "-0") {
		return 0, 0, errors.New("invalid bencoded integer: negative zero")
	}

	// Ensure only '-' (at most one, at the start) and digits are present.
	for i, ch := range valueString {
		if i == 0 && ch == '-' {
			continue
		}
		if ch < '0' || ch > '9' {
			return 0, 0, fmt.Errorf("invalid bencoded integer: unexpected character %q", ch)
		}
	}

	// Parse as base-10, 64-bit integer.
	value, err := strconv.ParseInt(valueString, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid bencoded integer %q: %w", valueString, err)
	}

	// Bytes consumed: from 'i' up to and including 'e'.
	consumed := eIndex + 1

	return value, consumed, nil
}

// bencodeInteger encodes an int64 as "i<value>e".
func bencodeInteger(buf *bytes.Buffer, n int64) error {
	buf.WriteByte('i')
	buf.WriteString(strconv.FormatInt(n, 10))
	buf.WriteByte('e')
	return nil
}

// -------------------- LIST --------------------

// decodeBencodedList decodes a bencoded list: "l<value1><value2>...e".
// It returns a []interface{} and the total number of bytes consumed.
func decodeBencodedList(b []byte) ([]interface{}, int, error) {
	if len(b) < 2 || b[0] != 'l' {
		return nil, 0, errors.New("invalid bencoded list: must start with 'l'")
	}

	// Make this a non-nil empty slice so JSON encodes it as "[]"
	result := make([]interface{}, 0)

	i := 1 // start right after 'l'

	for {
		if i >= len(b) {
			return nil, 0, errors.New("invalid bencoded list: missing terminating 'e'")
		}

		if b[i] == 'e' {
			// End of list. Consume the 'e' and stop.
			i++
			break
		}

		// Decode the next value starting at b[i:].
		elem, consumed, err := decodeValue(b[i:])
		if err != nil {
			return nil, 0, err
		}

		result = append(result, elem)
		i += consumed
	}

	return result, i, nil
}

// bencodeList encodes a []interface{} as "l<value1><value2>...e".
func bencodeList(buf *bytes.Buffer, list []interface{}) error {
	buf.WriteByte('l')
	for _, elem := range list {
		if err := encodeValue(buf, elem); err != nil {
			return err
		}
	}
	buf.WriteByte('e')
	return nil
}

// -------------------- DICTIONARY --------------------

// decodeBencodedDic decodes a bencoded dictionary: "d<key1><value1>...e".
// It returns a map[string]interface{} and the total number of bytes consumed.
func decodeBencodedDict(b []byte) (map[string]interface{}, int, error) {
	if len(b) < 2 || b[0] != 'd' {
		return nil, 0, errors.New("invalid bencoded dictionary: must start with 'd'")
	}

	// Make this a non-nil empty map so JSON encodes it as "{}"
	result := make(map[string]interface{})

	i := 1 // start right after 'd'

	for {
		if i >= len(b) {
			return nil, 0, errors.New("invalid bencoded dictionary: missing terminating 'e'")
		}

		if b[i] == 'e' {
			// End of list. Consume the 'e' and stop.
			i++
			break
		}

		// Decode the key string
		key, consumed, err := decodeBencodedString(b[i:])
		if err != nil {
			return nil, 0, err
		}
		if consumed <= 0 {
			return nil, 0, errors.New("error when decoding dictionary: key decoder consumed no bytes")
		}
		i += consumed

		// Decode the value corresponding to this key.
		value, consumed, err := decodeValue(b[i:])
		if err != nil {
			return nil, 0, err
		}
		if consumed <= 0 {
			return nil, 0, errors.New("error when decoding dictionary: value decoder consumed no bytes")
		}
		i += consumed

		result[key] = value
	}

	return result, i, nil
}

// encodeBencodedDict encodes a map[string]interface{} as
// "d<key1><value1>...e", with keys sorted lexicographically as required by
// the bencode/BitTorrent spec.
func bencodeDict(buf *bytes.Buffer, dict map[string]interface{}) error {
	buf.WriteByte('d')

	// Collect and sort keys to ensure lexicographical ordering
	keys := make([]string, 0, len(dict))
	for k := range dict {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if err := bencodeBytes(buf, []byte(k)); err != nil {
			return err
		}
		if err := encodeValue(buf, dict[k]); err != nil {
			return err
		}
	}

	buf.WriteByte('e')
	return nil
}
