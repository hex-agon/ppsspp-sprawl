package titledb

import (
	"sync"
)

// Certain games released in different regions are protocol compatible with each other despite having different titleIds.

var compatibilityMap = map[TitleId]TitleId{
	TitleId([]byte("ULES01213")): TitleId([]byte("ULUS10391")),
}
var mu sync.RWMutex

// CompatibleTitleId replaces the given titleId with the compatible titleId
func CompatibleTitleId(tid TitleId) (TitleId, bool) {
	mu.RLock()
	defer mu.RUnlock()

	id, ok := compatibilityMap[tid]
	return id, ok
}
