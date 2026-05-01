package titledb

const TitleIdLen = 9

type TitleId [TitleIdLen]byte

func NewTitleId(str string) TitleId {
	var tid TitleId
	copy(tid[:], str)
	return tid
}

func (t *TitleId) String() string {
	return string(t[:])
}

func (t *TitleId) Valid() bool {
	for _, char := range t {
		if (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}
