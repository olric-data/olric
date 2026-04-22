# Modifications from upstream olric-data/olric@v0.7.3

This fork adds a `CompareAndSwap` primitive to the Olric DMap API,
plus supporting RESP-protocol command, server-side handler, and
client-side bindings.

Modified/added files:
- internal/protocol/commands.go, internal/protocol/dmap.go (RESP command)
- internal/dmap/atomic.go, atomic_handlers.go, atomic_test.go, handlers.go (server)
- client.go, cluster_client.go, embedded_client.go, embedded_client_test.go (client)
- get_response.go (RawValue accessor)

Maintained by max007-008. See git history for per-commit detail.
