package dmap

import "context"

func (dm *DMap) MGet(ctx context.Context, keys ...string) map[string][]byte {
	result := make(map[string][]byte)

	for _, key := range keys {
		entry, err := dm.Get(ctx, key)

		if err == nil {
			result[key] = entry.Value()
		}
	}

	return result
}
