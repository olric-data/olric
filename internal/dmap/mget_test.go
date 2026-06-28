package dmap

import (
	"context"
	"testing"

	"github.com/olric-data/olric/internal/testcluster"
	"github.com/olric-data/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDmap_MGet(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	ctx := context.Background()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	keysInserted := make([]string, 0, 10)

	expected := make(map[string]string)

	for i := 0; i < 10; i++ {
		k := testutil.ToKey(i)
		v := testutil.ToVal(i)

		err = dm.Put(ctx, k, v, nil)
		require.NoError(t, err)

		keysInserted = append(keysInserted, k)
		expected[k] = string(v)
	}

	got := dm.MGet(ctx, keysInserted...)

	require.Len(t, got, len(keysInserted))

	for _, k := range keysInserted {
		v, ok := got[k]
		require.True(t, ok, "missing key: %s", k)

		require.Equal(t, expected[k], v, "mismatch for key: %s", k)
	}
}
