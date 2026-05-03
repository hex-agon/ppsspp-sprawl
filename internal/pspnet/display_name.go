package pspnet

import "bytes"

const DisplayNameLen = 128

// DisplayName is a null-terminated string composed of a-z, A-Z, 0-9 characters with a maximum length of 128 bytes.
type DisplayName [DisplayNameLen]byte

func NewDisplayName(string string) DisplayName {
	var result DisplayName
	copy(result[:], string)
	return result
}

func (d *DisplayName) String() string {
	terminator := bytes.IndexByte(d[:], 0)
	if terminator == -1 {
		terminator = DisplayNameLen
	}
	return string(d[:terminator])
}

func (d *DisplayName) Valid() bool {
	const validDisplayNameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_#áàâãéêíóôõúçÁÀÂÃÉÊÍÓÔÕÚÇ"

	length := bytes.IndexByte(d[:], 0)

	if length < 1 {
		return false
	}

	for i := 0; i < length; i++ {
		if !bytes.ContainsAny(d[i:i+1], validDisplayNameChars) {
			return false
		}
	}
	return true
}
