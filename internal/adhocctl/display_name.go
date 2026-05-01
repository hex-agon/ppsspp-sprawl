package adhocctl

import "bytes"

const displayNameLen = 128

// displayName is a null-terminated string composed of a-z, A-Z, 0-9 characters with a maximum length of 128 bytes.
type displayName [displayNameLen]byte

func newDisplayName(string string) displayName {
	var result displayName
	copy(result[:], string)
	return result
}

func (d *displayName) String() string {
	terminator := bytes.IndexByte(d[:], 0)
	if terminator == -1 {
		terminator = displayNameLen
	}
	return string(d[:terminator])
}

func (d *displayName) valid() bool {
	length := bytes.IndexByte(d[:], 0)

	if length < 1 {
		return false
	}

	for i := 0; i < length; i++ {
		char := d[i]
		// TODO: I think there's some special characters that are allowed here too (- and _ comes to mind)
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}
