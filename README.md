# Olric [![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Olric%3A+Distributed+and+in-memory+key%2Fvalue+database.+It+can+be+used+both+as+an+embedded+Go+library+and+as+a+language-independent+service.+&url=https://github.com/buraksezer/olric&hashtags=golang,distributed,database)

[![Go Reference](https://pkg.go.dev/badge/github.com/buraksezer/olric.svg)](https://pkg.go.dev/github.com/buraksezer/olric) [![Coverage Status](https://coveralls.io/repos/github/buraksezer/olric/badge.svg?branch=master)](https://coveralls.io/github/buraksezer/olric?branch=master) [![Build Status](https://travis-ci.org/buraksezer/olric.svg?branch=master)](https://travis-ci.org/buraksezer/olric) [![Go Report Card](https://goreportcard.com/badge/github.com/buraksezer/olric)](https://goreportcard.com/report/github.com/buraksezer/olric) [![Discord](https://img.shields.io/discord/721708998021087273.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2)](https://discord.gg/ahK7Vjr8We) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Olric is a distributed, in-memory key/value store and cache. It's designed from the ground up to be distributed, and it can be 
used both as an embedded Go library and as a language-independent service.

With Olric, you can instantly create a fast, scalable, shared pool of RAM across a cluster of computers.

Olric is implemented in [Go](https://go.dev/) and uses the [Redis serialization protocol](https://redis.io/topics/protocol). So Olric has client implementations in all major programming 
languages.

Olric is highly scalable and available. Distributed applications can use it for distributed caching, clustering and 
publish-subscribe messaging.

It is designed to scale out to hundreds of members and thousands of clients. When you add new members, they automatically 
discover the cluster and linearly increase the memory capacity. Olric offers simple scalability, partitioning (sharding), 
and re-balancing out-of-the-box. It does not require any extra coordination processes. With Olric, when you start another 
process to add more capacity, data and backups are automatically and evenly balanced. 

See [Docker](#docker) and [Samples](#samples) sections to get started! 

Join our [Discord server!](https://discord.gg/ahK7Vjr8We)

The current production version is [v0.5.7](https://github.com/buraksezer/olric/tree/release/v0.5.0#olric-)

### About versions

Olric v0.4 and previous versions use *Olric Binary Protocol*, v0.5.x and later use [Redis serialization protocol](https://redis.io/docs/reference/protocol-spec/) for communication and the API was significantly changed.
Olric v0.4.x tree is going to receive bug fixes and security updates forever, but I would recommend considering an upgrade to the new version.

This document only covers `v0.5.x` and later. See v0.4.x documents [here](https://github.com/buraksezer/olric/tree/release/v0.4.0#olric-).

## At a glance

* Designed to share some transient, approximate, fast-changing data between servers,
* Uses Redis serialization protocol,
* Implements a distributed hash table,
* Provides a drop-in replacement for Redis Publish/Subscribe messaging system,
* Supports both programmatic and declarative configuration, 
* Embeddable but can be used as a language-independent service with *olricd*,
* Supports different eviction algorithms (including LRU and TTL),
* Highly available and horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Supports replication by default (with sync and async options),
* Quorum-based voting for replica control (Read/Write quorums),
* Supports atomic operations,
* Provides an iterator on distributed maps,
* Provides a plugin interface for service discovery daemons,
* Provides a locking primitive which inspired by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode),

## Possible Use Cases

Olric is an eventually consistent, unordered key/value data store. It supports various eviction mechanisms for distributed caching implementations. Olric 
also provides publish-subscribe messaging, data replication, failure detection and simple anti-entropy services. 

It's good at distributed caching and publish/subscribe messaging.

## Table of Contents

* [Features](#features)
* [Support](#support)
* [Installing](#installing)
  * [Docker](#docker)
  * [Kubernetes](#kubernetes)
  * [Working with Docker Compose](#working-with-docker-compose)
* [Getting Started](#getting-started)
  * [Operation Modes](#operation-modes)
    * [Embedded Member](#embedded-member)
    * [Client-Server](#client-server)
* [Golang Client](#golang-client)
* [Cluster Events](#cluster-events)
* [Commands](#commands)
  * [Distributed Map](#distributed-map)
    * [DM.PUT](#dmput)
    * [DM.GET](#dmget)
    * [DM.DEL](#dmdel)
    * [DM.EXPIRE](#dmexpire)
    * [DM.PEXPIRE](#dmpexpire)
    * [DM.DESTROY](#dmdestroy)
    * [Atomic Operations](#atomic-operations)
      * [DM.INCR](#dmincr)
      * [DM.DECR](#dmdecr)
      * [DM.GETPUT](#dmgetput)
      * [DM.INCRBYFLOAT](#dmincrbyfloat)
    * [Locking](#locking)
      * [DM.LOCK](#dmlock)
      * [DM.UNLOCK](#dmunlock)
      * [DM.LOCKLEASE](#dmlocklease)
      * [DM.PLOCKLEASE](#dmplocklease)
    * [DM.SCAN](#dmscan)
  * [Publish-Subscribe](#publish-subscribe)
    * [SUBSCRIBE](#subscribe)
    * [PSUBSCRIBE](#psubscribe)
    * [UNSUBSCRIBE](#unsubscribe)
    * [PUNSUBSCRIBE](#punsubscribe)
    * [PUBSUB CHANNELS](#pubsub-channels)
    * [PUBSUB NUMPAT](#pubsub-numpat)
    * [PUBSUB NUMSUB](#pubsub-numsub)
    * [QUIT](#quit)
    * [PING](#ping)
  * [Cluster](#cluster)
    * [CLUSTER.ROUTINGTABLE](#clusterroutingtable)
    * [CLUSTER.MEMBERS](#clustermembers)
  * [Others](#others)
    * [PING](#ping)
    * [STATS](#stats)
* [Configuration](#configuration)
    * [Embedded Member Mode](#embedded-member-mode)
      * [Manage the configuration in YAML format](#manage-the-configuration-in-yaml-format)
    * [Client-Server Mode](#client-server-mode)
    * [Network Configuration](#network-configuration)
    * [Service discovery](#service-discovery)
    * [Timeouts](#timeouts)
* [Architecture](#architecture)
  * [Overview](#overview)
  * [Consistency and Replication Model](#consistency-and-replication-model)
    * [Last-write-wins conflict resolution](#last-write-wins-conflict-resolution)
    * [PACELC Theorem](#pacelc-theorem)
    * [Read-Repair on DMaps](#read-repair-on-dmaps)
    * [Quorum-based Replica Control](#quorum-based-replica-control)
    * [Simple Split-Brain Protection](#simple-split-brain-protection)
  * [Eviction](#eviction)
    * [Expire with TTL](#expire-with-ttl)
    * [Expire with MaxIdleDuration](#expire-with-maxidleduration)
    * [Expire with LRU](#expire-with-lru)
  * [Lock Implementation](#lock-implementation)
  * [Storage Engine](#storage-engine)
* [Samples](#samples)
* [Contributions](#contributions)
* [License](#license)
* [About the name](#about-the-name)


## Features

* Designed to share some transient, approximate, fast-changing data between servers,
* Accepts arbitrary types as value,
* Only in-memory,
* Uses Redis protocol,
* Compatible with existing Redis clients,
* Embeddable but can be used as a language-independent service with olricd,
* GC-friendly storage engine,
* O(1) running time for lookups,
* Supports atomic operations,
* Provides a lock implementation which can be used for non-critical purposes,
* Different eviction policies: LRU, MaxIdleDuration and Time-To-Live (TTL),
* Highly available,
* Horizontally scalable,
* Provides best-effort consistency guarantees without being a complete CP (indeed PA/EC) solution,
* Distributes load fairly among cluster members with a [consistent hash function](https://github.com/buraksezer/consistent),
* Supports replication by default (with sync and async options),
* Quorum-based voting for replica control,
* Thread-safe by default,
* Provides an iterator on distributed maps,
* Provides a plugin interface for service discovery daemons and cloud providers,
* Provides a locking primitive which inspired by [SETNX of Redis](https://redis.io/commands/setnx#design-pattern-locking-with-codesetnxcode),
* Provides a drop-in replacement of Redis' Publish-Subscribe messaging feature.

See [Architecture](#architecture) section to see details.

## Support

We have a few communication channels: 

* [Issue Tracker](https://github.com/buraksezer/olric/issues)
* [Discord server](https://discord.gg/ahK7Vjr8We)

You should know that the issue tracker is only intended for bug reports and feature requests.

Software doesn't maintain itself. If you need support on complex topics or request new features, please consider [sponsoring Olric](https://github.com/sponsors/buraksezer).

## Installing

With a correctly configured Golang environment:

```
go install github.com/buraksezer/olric/cmd/olricd@v0.5.7
```

Now you can start using Olric:

```
olricd -c cmd/olricd/olricd-local.yaml
```

See [Configuration](#configuration) section to create your cluster properly.

### Docker

You can launch `olricd` Docker container by running the following command. 

```bash
docker run -p 3320:3320 olricio/olricd:v0.5.4
``` 

This command will pull olricd Docker image and run a new Olric Instance. You should know that the container exposes 
`3320` and `3322` ports. 

Now, you can access an Olric cluster using any Redis client including `redis-cli`:

```bash
redis-cli -p 3320
127.0.0.1:3320> DM.PUT my-dmap my-key "Olric Rocks!"
OK
127.0.0.1:3320> DM.GET my-dmap my-key
"Olric Rocks!"
127.0.0.1:3320>
```

## Getting Started

With olricd, you can create an Olric cluster with a few commands. This is how to install olricd:

```bash
go install github.com/buraksezer/olric/cmd/olricd@v0.5.7
```

Let's create a cluster with the following:

```
olricd -c <YOUR_CONFIG_FILE_PATH>
```

You can find the sample configuration file under `cmd/olricd/olricd-local.yaml`. It can perfectly run with single node. 
olricd also supports `OLRICD_CONFIG` environment variable to set configuration. Just like that: 

```
OLRICD_CONFIG=<YOUR_CONFIG_FILE_PATH> olricd
```

Olric uses [hashicorp/memberlist](https://github.com/hashicorp/memberlist) for failure detection and cluster membership. 
Currently, there are different ways to discover peers in a cluster. You can use a static list of nodes in your configuration. 
It's ideal for development and test environments. Olric also supports Consul, Kubernetes and all well-known cloud providers
for service discovery. Please take a look at [Service Discovery](#service-discovery) section for further information.

See [Client-Server](#client-server) section to get more information about this deployment scenario.

#### Maintaining a list of peers manually

Basically, there is a list of nodes under `memberlist` block in the configuration file. In order to create an Olric cluster, 
you just need to add `Host:Port` pairs of the other nodes. Please note that the `Port` is the memberlist port of the peer.
It is `3322` by default. 

```yaml
memberlist:
  peers:
    - "localhost:3322"
```

Thanks to [hashicorp/memberlist](https://github.com/hashicorp/memberlist), Olric nodes can share the full list of members 
with each other. So an Olric node can discover the whole cluster by using a single member address.

#### Embedding into your Go application.

See [Samples](#samples) section to learn how to embed Olric into your existing Golang application.

### Operation Modes

Olric has two different operation modes.

#### Embedded Member

In Embedded Member Mode, members include both the application and Olric data and services. The advantage of the Embedded
Member Mode is having a low-latency data access and locality.

#### Client-Server

In Client-Server Mode, Olric data and services are centralized in one or more servers, and they are accessed by the 
application through clients. You can have a cluster of servers that can be independently created and scaled. Your clients 
communicate with these members to reach to Olric data and services on them.

Client-Server deployment has advantages including more predictable and reliable performance, easier identification
of problem causes and, most importantly, better scalability. When you need to scale in this deployment type, just add more
Olric server members. You can address client and server scalability concerns separately.

## Golang Client

The official Golang client is defined by the `Client` interface. There are two different implementations of that interface in 
this repository. `EmbeddedClient` provides a client implementation for [embedded-member](#embedded-member) scenario, 
`ClusterClient` provides an implementation of the same interface for [client-server](#client-server) deployment scenario. 
Obviously, you can use `ClusterClient` for your embedded-member deployments. But it's good to use `EmbeddedClient` provides 
a better performance due to localization of the queries.

See the client documentation on [pkg.go.dev](https://pkg.go.dev/github.com/buraksezer/olric@v0.5.7)

## Cluster Events

Olric can send push cluster events to `cluster.events` channel. Available cluster events:

* node-join-event
* node-left-event
* fragment-migration-event
* fragment-received-even

If you want to receive these events, set `true` to `EnableClusterEventsChannel` and subscribe to `cluster.events` channel. 
The default is `false`.

See [events/cluster_events.go](events/cluster_events.go) file to get more information about events.

## Commands

Olric uses Redis protocol and supports Redis-style commands to query the database. You can use any Redis client, including
`redis-cli`. The official Go client is a thin layer around [go-redis/redis](https://github.com/go-redis/redis) package. 
See [Golang Client](#golang-client) section for the documentation.

### Distributed Map

#### DM.PUT 

DM.PUT sets the value for the given key. It overwrites any previous value for that key.

```
DM.PUT dmap key value [ EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds ] [ NX | XX]
```

**Example:**
```
127.0.0.1:3320> DM.PUT my-dmap my-key value
OK
```

**Options:**

The DM.PUT command supports a set of options that modify its behavior:

* **EX** *seconds* -- Set the specified expire time, in seconds.
* **PX** *milliseconds* -- Set the specified expire time, in milliseconds.
* **EXAT** *timestamp-seconds* -- Set the specified Unix time at which the key will expire, in seconds.
* **PXAT** *timestamp-milliseconds* -- Set the specified Unix time at which the key will expire, in milliseconds.
* **NX** -- Only set the key if it does not already exist.
* **XX** -- Only set the key if it already exist.

**Return:**

* **Simple string reply:** OK if DM.PUT was executed correctly.
* **KEYFOUND:** (error) if the DM.PUT operation was not performed because the user specified the NX option but the condition was not met.
* **KEYNOTFOUND:** (error) if the DM.PUT operation was not performed because the user specified the XX option but the condition was not met.

#### DM.GET

DM.GET gets the value for the given key. It returns (error)`KEYNOTFOUND` if the key doesn't exist. 

```
DM.GET dmap key
```

**Example:**

```
127.0.0.1:3320> DM.GET dmap key
"value"
```

**Return:**

**Bulk string reply**: the value of key, or (error)`KEYNOTFOUND` when key does not exist.

#### DM.DEL

DM.DEL deletes values for the given keys. It doesn't return any error if the key does not exist.

```
DM.DEL dmap key [key...]
```

**Example:**

```
127.0.0.1:3320> DM.DEL dmap key1 key2
(integer) 2
```

**Return:**

* **Integer reply**: The number of keys that were removed.

#### DM.EXPIRE

DM.EXPIRE updates or sets the timeout for the given key. It returns `KEYNOTFOUND` if the key doesn't exist. After the timeout has expired, 
the key will automatically be deleted. 

The timeout will only be cleared by commands that delete or overwrite the contents of the key, including DM.DEL, DM.PUT, DM.GETPUT.

```
DM.EXPIRE dmap key seconds
```

**Example:**

```
127.0.0.1:3320> DM.EXPIRE dmap key 1
OK
```

**Return:**

* **Simple string reply:** OK if DM.EXPIRE was executed correctly.
* **KEYNOTFOUND:** (error) when key does not exist.

#### DM.PEXPIRE

DM.PEXPIRE updates or sets the timeout for the given key. It returns `KEYNOTFOUND` if the key doesn't exist. After the timeout has expired,
the key will automatically be deleted.

The timeout will only be cleared by commands that delete or overwrite the contents of the key, including DM.DEL, DM.PUT, DM.GETPUT.

```
DM.PEXPIRE dmap key milliseconds
```

**Example:**

```
127.0.0.1:3320> DM.PEXPIRE dmap key 1000
OK
```

**Return:**

* **Simple string reply:** OK if DM.EXPIRE was executed correctly.
* **KEYNOTFOUND:** (error) when key does not exist.

#### DM.DESTROY

DM.DESTROY flushes the given DMap on the cluster. You should know that there is no global lock on DMaps. DM.PUT and DM.DESTROY commands
may run concurrently on the same DMap. 

```
DM.DESTROY dmap
```

**Example:**

```
127.0.0.1:3320> DM.DESTROY dmap
OK
```

**Return:**

* **Simple string reply:** OK, if DM.DESTROY was executed correctly.

### Atomic Operations

Operations on key/value pairs are performed by the partition owner. In addition, atomic operations are guarded by a lock implementation which can be found under `internal/locker`. It means that
Olric guaranties consistency of atomic operations, if there is no network partition. Basic flow for `DM.INCR`:

* Acquire the lock for the given key,
* Call `DM.GET` to retrieve the current value,
* Calculate the new value,
* Call `DM.PUT` to set the new value,
* Release the lock.

It's important to know that if you call `DM.PUT` and `DM.GETPUT` concurrently on the same key, this will break the atomicity.

`internal/locker` package is provided by [Docker](https://github.com/moby/moby).

**Important note about consistency:**

You should know that Olric is a PA/EC (see [Consistency and Replication Model](#consistency-and-replication-model)) product. So if your network is stable, all the operations on key/value
pairs are performed by a single cluster member. It means that you can be sure about the consistency when the cluster is stable. It's important to know that computer networks fail
occasionally, processes crash and random GC pauses may happen. Many factors can lead a network partitioning. If you cannot tolerate losing strong consistency under network partitioning,
you need to use a different tool for atomic operations.

See [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html) and [Jepsen Analysis on Hazelcast 3.8.3](https://hazelcast.com/blog/jepsen-analysis-hazelcast-3-8-3/) for more insight on this topic.


#### DM.INCR

DM.INCR atomically increments the number stored at key by delta. The return value is the new value after being incremented or an error.

```
DM.INCR dmap key delta
```

**Example:**

```
127.0.0.1:3320> DM.INCR dmap key 10
(integer) 10
```

**Return:**

* **Integer reply:** the value of key after the increment.

#### DM.DECR

DM.DECR atomically decrements the number stored at key by delta. The return value is the new value after being incremented or an error.

```
DM.DECR dmap key delta
```

**Example:**

```
127.0.0.1:3320> DM.DECR dmap key 10
(integer) 0
```

**Return:**

* **Integer reply:** the value of key after the increment.

#### DM.GETPUT

DM.GETPUT atomically sets key to value and returns the old value stored at the key.

```
DM.GETPUT dmap key value
```

**Example:**

```
127.0.0.1:3320> DM.GETPUT dmap key value-1
(nil)
127.0.0.1:3320> DM.GETPUT dmap key value-2
"value-1"
```

**Return:**

* **Bulk string reply**: the old value stored at the key.

#### DM.INCRBYFLOAT

DM.INCRBYFLOAT atomically increments the number stored at key by delta. The return value is the new value after being incremented or an error.

```
DM.INCRBYFLOAT dmap key delta
```

**Example:**

```
127.0.0.1:3320> DM.PUT dmap key 10.50
OK
127.0.0.1:3320> DM.INCRBYFLOAT dmap key 0.1
"10.6"
127.0.0.1:3320> DM.PUT dmap key 5.0e3
OK
127.0.0.1:3320> DM.INCRBYFLOAT dmap key 2.0e2
"5200"
```

**Return:**

* **Bulk string reply**: the value of key after the increment.


### Locking

**Important:** The lock provided by DMap implementation is approximate and only to be used for non-critical purposes.

The DMap implementation is already thread-safe to meet your thread safety requirements. When you want to have more control on the
concurrency, you can use **DM.LOCK** command. Olric borrows the locking algorithm from Redis. Redis authors propose
the following algorithm:

> The command <SET resource-name anystring NX EX max-lock-time> is a simple way to implement a locking system with Redis.
>
> A client can acquire the lock if the above command returns OK (or retry after some time if the command returns Nil), and remove the lock just using DEL.
>
> The lock will be auto-released after the expire time is reached.
>
> It is possible to make this system more robust modifying the unlock schema as follows:
>
> Instead of setting a fixed string, set a non-guessable large random string, called token.
> Instead of releasing the lock with DEL, send a script that only removes the key if the value matches.
> This avoids that a client will try to release the lock after the expire time deleting the key created by another client that acquired the lock later.

Equivalent of `SETNX` command in Olric is `DM.PUT dmap key value NX`. DM.LOCK command are properly implements
the algorithm which is proposed above.

You should know that this implementation is subject to the clustering algorithm. So there is no guarantee about reliability in the case of network partitioning. I recommend the lock implementation to be used for
efficiency purposes in general, instead of correctness.

**Important note about consistency:**

You should know that Olric is a PA/EC (see [Consistency and Replication Model](#consistency-and-replication-model)) product. So if your network is stable, all the operations on key/value
pairs are performed by a single cluster member. It means that you can be sure about the consistency when the cluster is stable. It's important to know that computer networks fail
occasionally, processes crash and random GC pauses may happen. Many factors can lead a network partitioning. If you cannot tolerate losing strong consistency under network partitioning,
you need to use a different tool for locking.

See [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html) and [Jepsen Analysis on Hazelcast 3.8.3](https://hazelcast.com/blog/jepsen-analysis-hazelcast-3-8-3/) for more insight on this topic.

#### DM.LOCK

DM.LOCK sets a lock for the given key. The acquired lock is only valid for the key in this DMap.
It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.

DM.LOCK returns a token. You must keep that token to unlock the key. Using prefixed keys is highly recommended.
If the key does already exist in the DMap, DM.LOCK will wait until the deadline is exceeded.

```
DM.LOCK dmap key seconds [ EX seconds | PX milliseconds ]
```

**Options:**

* **EX** *seconds* -- Set the specified expire time, in seconds.
* **PX** *milliseconds* -- Set the specified expire time, in milliseconds.

**Example:**

```
127.0.0.1:3320> DM.LOCK dmap lock.key 10
2363ec600be286cb10fbb35181efb029
```

**Return:**

* **Simple string reply:** a token to unlock or lease the lock.
* **NOSUCHLOCK**: (error) returned when the requested lock does not exist.
* **LOCKNOTACQUIRED**: (error) returned when the requested lock could not be acquired.

#### DM.UNLOCK

DM.UNLOCK releases an acquired lock for the given key. It returns `NOSUCHLOCK` if there is no lock for the given key.

```
DM.UNLOCK dmap key token
```

**Example:**

```
127.0.0.1:3320> DM.UNLOCK dmap key 2363ec600be286cb10fbb35181efb029
OK
```

**Return:**

* **Simple string reply:** OK if DM.UNLOCK was executed correctly.
* **NOSUCHLOCK**: (error) returned when the lock does not exist.

#### DM.LOCKLEASE

DM.LOCKLEASE sets or updates the timeout of the acquired lock for the given key. It returns `NOSUCHLOCK` if there is no lock for the given key.

DM.LOCKLEASE accepts seconds as timeout.

```
DM.LOCKLEASE dmap key token seconds
```

**Example:**

```
127.0.0.1:3320> DM.LOCKLEASE dmap key 2363ec600be286cb10fbb35181efb029 100
OK
```

**Return:**

* **Simple string reply:** OK if DM.UNLOCK was executed correctly.
* **NOSUCHLOCK**: (error) returned when the lock does not exist.

#### DM.PLOCKLEASE

DM.PLOCKLEASE sets or updates the timeout of the acquired lock for the given key. It returns `NOSUCHLOCK` if there is no lock for the given key.

DM.PLOCKLEASE accepts milliseconds as timeout.

```
DM.LOCKLEASE dmap key token milliseconds
```

**Example:**

```
127.0.0.1:3320> DM.PLOCKLEASE dmap key 2363ec600be286cb10fbb35181efb029 1000
OK
```

**Return:**

* **Simple string reply:** OK if DM.PLOCKLEASE was executed correctly.
* **NOSUCHLOCK**: (error) returned when the lock does not exist.

#### DM.SCAN

DM.SCAN is a cursor based iterator. This means that at every call of the command, the server returns an updated cursor 
that the user needs to use as the cursor argument in the next call.

An iteration starts when the cursor is set to 0, and terminates when the cursor returned by the server is 0. The iterator runs
locally on every partition. So you need to know the partition count. If the returned cursor is 0 for a particular partition,
you have to start scanning the next partition. 

```
DM.SCAN partID dmap cursor [ MATCH pattern | COUNT count ]
```

**Example:**

```
127.0.0.1:3320> DM.SCAN 3 bench 0
1) "96990"
2)  1) "memtier-2794837"
    2) "memtier-8630933"
    3) "memtier-6415429"
    4) "memtier-7808686"
    5) "memtier-3347072"
    6) "memtier-4247791"
    7) "memtier-3931982"
    8) "memtier-7164719"
    9) "memtier-4710441"
   10) "memtier-8892916"
127.0.0.1:3320> DM.SCAN 3 bench 96990
1) "193499"
2)  1) "memtier-429905"
    2) "memtier-1271812"
    3) "memtier-7835776"
    4) "memtier-2717575"
    5) "memtier-95312"
    6) "memtier-2155214"
    7) "memtier-123931"
    8) "memtier-2902510"
    9) "memtier-2632291"
   10) "memtier-1938450"
```
### Publish-Subscribe

**SUBSCRIBE**, **UNSUBSCRIBE** and **PUBLISH** implement the Publish/Subscribe messaging paradigm where 
senders are not programmed to send their messages to specific receivers. Rather, published messages are characterized 
into channels, without knowledge of what (if any) subscribers there may be. Subscribers express interest in one or more 
channels, and only receive messages that are of interest, without knowledge of what (if any) publishers there are. 
This decoupling of publishers and subscribers can allow for greater scalability and a more dynamic network topology.

**Important note:** In an Olric cluster, clients can subscribe to every node, and can also publish to every other node. The cluster
will make sure that published messages are forwarded as needed.

*Source of this section: [https://redis.io/commands/?group=pubsub](https://redis.io/commands/?group=pubsub)*

#### SUBSCRIBE

Subscribes the client to the specified channels.

```
SUBSCRIBE channel [channel...]
```

Once the client enters the subscribed state it is not supposed to issue any other commands, except for additional **SUBSCRIBE**, 
**PSUBSCRIBE**, **UNSUBSCRIBE**, **PUNSUBSCRIBE**, **PING**, and **QUIT** commands.

#### PSUBSCRIBE

Subscribes the client to the given patterns.

```
PSUBSCRIBE pattern [ pattern ...]
```

Supported glob-style patterns:

* `h?llo` subscribes to hello, hallo and hxllo
* `h*llo` subscribes to hllo and heeeello
* `h[ae]llo` subscribes to hello and hallo, but not hillo
* Use **\\** to escape special characters if you want to match them verbatim.

#### UNSUBSCRIBE

Unsubscribes the client from the given channels, or from all of them if none is given.

```
UNSUBSCRIBE [channel [channel ...]]
```

When no channels are specified, the client is unsubscribed from all the previously subscribed channels. In this case, 
a message for every unsubscribed channel will be sent to the client.

#### PUNSUBSCRIBE

Unsubscribes the client from the given patterns, or from all of them if none is given.

```
PUNSUBSCRIBE [pattern [pattern ...]]
```

When no patterns are specified, the client is unsubscribed from all the previously subscribed patterns. In this case, 
a message for every unsubscribed pattern will be sent to the client.

#### PUBSUB CHANNELS

Lists the currently active channels.

```
PUBSUB CHANNELS [pattern]
```

An active channel is a Pub/Sub channel with one or more subscribers (excluding clients subscribed to patterns).

If no pattern is specified, all the channels are listed, otherwise if pattern is specified only channels matching the 
specified glob-style pattern are listed.

#### PUBSUB NUMPAT

Returns the number of unique patterns that are subscribed to by clients (that are performed using the PSUBSCRIBE command).

```
PUBSUB NUMPAT
```

Note that this isn't the count of clients subscribed to patterns, but the total number of unique patterns all the clients are subscribed to.

**Important note**: In an Olric cluster, clients can subscribe to every node, and can also publish to every other node. The cluster 
will make sure that published messages are forwarded as needed. That said, PUBSUB's replies in a cluster only report information 
from the node's Pub/Sub context, rather than the entire cluster.

#### PUBSUB NUMSUB

Returns the number of subscribers (exclusive of clients subscribed to patterns) for the specified channels.

```
PUBSUB NUMSUB [channel [channel ...]]
```
Note that it is valid to call this command without channels. In this case it will just return an empty list.

**Important note**: In an Olric cluster, clients can subscribe to every node, and can also publish to every other node. The cluster 
will make sure that published messages are forwarded as needed. That said, PUBSUB's replies in a cluster only report information 
from the node's Pub/Sub context, rather than the entire cluster.

#### QUIT

Ask the server to close the connection. The connection is closed as soon as all pending replies have been written to the client.

```
QUIT
```
### Cluster

#### CLUSTER.ROUTINGTABLE

CLUSTER.ROUTINGTABLE returns the latest view of the routing table. Simply, it's a data structure that maps
partitions to members.

```
CLUSTER.ROUTINGTABLE
```

**Example:**

```
127.0.0.1:3320> CLUSTER.ROUTINGTABLE
 1) 1) (integer) 0
     2) 1) "127.0.0.1:3320"
     3) (empty array)
  2) 1) (integer) 1
     2) 1) "127.0.0.1:3320"
     3) (empty array)
  3) 1) (integer) 2
     2) 1) "127.0.0.1:3320"
     3) (empty array)
```

It returns an array of arrays. 

**Fields:**

```
1) (integer) 0 <- Partition ID
  2) 1) "127.0.0.1:3320" <- Array of the current and previous primary owners
  3) (empty array) <- Array of backup owners. 
```

#### CLUSTER.MEMBERS

CLUSTER.MEMBERS returns an array of known members by the server.

```
CLUSTER.MEMBERS
```

**Example:**

```
127.0.0.1:3320> CLUSTER.MEMBERS
1) 1) "127.0.0.1:3320"
   2) (integer) 1652619388427137000
   3) "true"
```

**Fields:**

```
1) 1) "127.0.0.1:3320" <- Member's name in the cluster
   2) (integer) 1652619388427137000 <-Member's birthedate
   3) "true" <- Is cluster coordinator (the oldest node)
```

### Others

#### PING

Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk. This command is often used to
test if a connection is still alive, or to measure latency.

```
PING
```

#### STATS

The STATS command returns information and statistics about the server in JSON format. See `stats/stats.go` file.

## Configuration

Olric supports both declarative and programmatic configurations. You can choose one of them depending on your needs.
You should feel free to ask any questions about configuration and integration. Please see [Support](#support) section.

### Embedded-Member Mode

#### Programmatic Configuration
Olric provides a function to generate default configuration to use in embedded-member mode:

```go
import "github.com/buraksezer/olric/config"
...
c := config.New("local")
```

The `New` function takes a parameter called `env`. It denotes the network environment and consumed by [hashicorp/memberlist](https://github.com/hashicorp/memberlist). 
Default configuration is good enough for distributed caching scenario. In order to see all configuration parameters, please take a look at [this](https://godoc.org/github.com/buraksezer/olric/config).

See [Sample Code](#sample-code) section for an introduction.

#### Declarative configuration with YAML format

You can also import configuration from a YAML file by using the `Load` function:

```go
c, err := config.Load(path/to/olric.yaml)
```

A sample configuration file in YAML format can be found [here](https://github.com/buraksezer/olric/blob/master/cmd/olricd/olricd.yaml). This may be the most appropriate way to manage the Olric configuration.


### Client-Server Mode

Olric provides **olricd** to implement client-server mode. olricd gets a YAML file for the configuration. The most basic  functionality of olricd is that 
translating YAML configuration into Olric's configuration struct. A sample `olricd.yaml` file  is being provided [here](https://github.com/buraksezer/olric/blob/master/cmd/olricd/olricd.yaml).

### Network Configuration

In an Olric instance, there are two different TCP servers. One for Olric, and the other one is for memberlist. `BindAddr` is very
critical to deploy a healthy Olric node. There are different scenarios:

* You can freely set a domain name or IP address as `BindAddr` for both Olric and memberlist. Olric will resolve and use it to bind.
* You can freely set `localhost`, `127.0.0.1` or `::1` as `BindAddr` in development environment for both Olric and memberlist.
* You can freely set `0.0.0.0` as `BindAddr` for both Olric and memberlist. Olric will pick an IP address, if there is any.
* If you don't set `BindAddr`, hostname will be used, and it will be resolved to get a valid IP address.
* You can set a network interface by using `Config.Interface` and `Config.MemberlistInterface` fields. Olric will find an appropriate IP address for the given interfaces, if there is any.
* You can set both `BindAddr` and interface parameters. In this case Olric will ensure that `BindAddr` is available on the given interface.

You should know that Olric needs a single and stable IP address to function properly. If you don't know the IP address of the host at the deployment time, 
you can set `BindAddr` as `0.0.0.0`. Olric will very likely to find an IP address for you.

### Service Discovery

Olric provides a service discovery interface which can be used to implement plugins. 

We currently have a bunch of service discovery plugins for automatic peer discovery on cloud environments:

* [buraksezer/olric-consul-plugin](https://github.com/buraksezer/olric-consul-plugin) provides a plugin using Consul.
* [buraksezer/olric-cloud-plugin](https://github.com/buraksezer/olric-cloud-plugin) provides a plugin for well-known cloud providers. Including Kubernetes.
* [justinfx/olric-nats-plugin](https://github.com/justinfx/olric-nats-plugin) provides a plugin using nats.io

In order to get more info about installation and configuration of the plugins, see their GitHub page. 

### Timeouts

Olric nodes supports setting `KeepAlivePeriod` on TCP sockets. 

**Server-side:**

##### config.KeepAlivePeriod 

KeepAlivePeriod denotes whether the operating system should send keep-alive messages on the connection.

**Client-side:**
 
##### config.DialTimeout

Timeout for TCP dial. The timeout includes name resolution, if required. When using TCP, and the host in the address 
parameter resolves to multiple IP addresses, the timeout is spread over each consecutive dial, such that each is
given an appropriate fraction of the time to connect.

##### config.ReadTimeout

Timeout for socket reads. If reached, commands will fail with a timeout instead of blocking. Use value -1 for no 
timeout and 0 for default. The default is config.DefaultReadTimeout

##### config.WriteTimeout

Timeout for socket writes. If reached, commands will fail with a timeout instead of blocking. The default is config.DefaultWriteTimeout

## Architecture

### Overview

Olric uses:
* [hashicorp/memberlist](https://github.com/hashicorp/memberlist) for cluster membership and failure detection,
* [buraksezer/consistent](https://github.com/buraksezer/consistent) for consistent hashing and load balancing,
* [Redis Serialization Protocol](https://github.com/tidwall/redcon) for communication.

Olric distributes data among partitions. Every partition is being owned by a cluster member and may have one or more backups for redundancy. 
When you read or write a DMap entry, you transparently talk to the partition owner. Each request hits the most up-to-date version of a
particular data entry in a stable cluster.

In order to find the partition which the key belongs to, Olric hashes the key and mod it with the number of partitions:

```
partID = MOD(hash result, partition count)
```

The partitions are being distributed among cluster members by using a consistent hashing algorithm. In order to get details, please see [buraksezer/consistent](https://github.com/buraksezer/consistent). 

When a new cluster is created, one of the instances is elected as the **cluster coordinator**. It manages the partition table: 

* When a node joins or leaves, it distributes the partitions and their backups among the members again,
* Removes empty previous owners from the partition owners list,
* Pushes the new partition table to all the members,
* Pushes the partition table to the cluster periodically.

Members propagate their birthdate(POSIX time in nanoseconds) to the cluster. The coordinator is the oldest member in the cluster.
If the coordinator leaves the cluster, the second oldest member gets elected as the coordinator.

Olric has a component called **rebalancer** which is responsible for keeping underlying data structures consistent:

* Works on every node,
* When a node joins or leaves, the cluster coordinator pushes the new partition table. Then, the **rebalancer** runs immediately and moves the partitions and backups to their new hosts,
* Merges fragmented partitions.

Partitions have a concept called **owners list**. When a node joins or leaves the cluster, a new primary owner may be assigned by the 
coordinator. At any time, a partition may have one or more partition owners. If a partition has two or more owners, this is called **fragmented partition**. 
The last added owner is called **primary owner**. Write operation is only done by the primary owner. The previous owners are only used for read and delete.

When you read a key, the primary owner tries to find the key on itself, first. Then, queries the previous owners and backups, respectively.
The delete operation works the same way.

The data(distributed map objects) in the fragmented partition is moved slowly to the primary owner by the **rebalancer**. Until the move is done,
the data remains available on the previous owners. The DMap methods use this list to query data on the cluster.

*Please note that, 'multiple partition owners' is an undesirable situation and the **rebalancer** component is designed to fix that in a short time.*

### Consistency and Replication Model

**Olric is an AP product** in the context of [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem), which employs the combination of primary-copy 
and [optimistic replication](https://en.wikipedia.org/wiki/Optimistic_replication) techniques. With optimistic replication, when the partition owner 
receives a write or delete operation for a key, applies it locally, and propagates it to the backup owners.

This technique enables Olric clusters to offer high throughput. However, due to temporary situations in the system, such as network
failure, backup owners can miss some updates and diverge from the primary owner. If a partition owner crashes while there is an
inconsistency between itself and the backups, strong consistency of the data can be lost.

Two types of backup replication are available: **sync** and **async**. Both types are still implementations of the optimistic replication
model.

* **sync**: Blocks until write/delete operation is applied by backup owners.
* **async**: Just fire & forget.

#### Last-write-wins conflict resolution

Every time a piece of data is written to Olric, a timestamp is attached by the client. Then, when Olric has to deal with conflict data in the case 
of network partitioning, it simply chooses the data with the most recent timestamp. This called LWW conflict resolution policy.

#### PACELC Theorem

From Wikipedia:

> In theoretical computer science, the [PACELC theorem](https://en.wikipedia.org/wiki/PACELC_theorem) is an extension to the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem). It states that in case of network partitioning (P) in a 
> distributed computer system, one has to choose between availability (A) and consistency (C) (as per the CAP theorem), but else (E), even when the system is 
> running normally in the absence of partitions, one has to choose between latency (L) and consistency (C).

In the context of PACELC theorem, Olric is a **PA/EC** product. It means that Olric is considered to be **consistent** data store if the network is stable. 
Because the key space is divided between partitions and every partition is controlled by its primary owner. All operations on DMaps are redirected to the 
partition owner. 

In the case of network partitioning, Olric chooses **availability** over consistency. So that you can still access some parts of the cluster when the network is unreliable, 
but the cluster may return inconsistent results.  

Olric implements read-repair and quorum based voting system to deal with inconsistencies in the DMaps. 

Readings on PACELC theorem:
* [Please stop calling databases CP or AP](https://martin.kleppmann.com/2015/05/11/please-stop-calling-databases-cp-or-ap.html)
* [Problems with CAP, and Yahoo’s little known NoSQL system](https://dbmsmusings.blogspot.com/2010/04/problems-with-cap-and-yahoos-little.html)
* [A Critique of the CAP Theorem](https://arxiv.org/abs/1509.05393)
* [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html)

#### Read-Repair on DMaps

Read repair is a feature that allows for inconsistent data to be fixed at query time. Olric tracks every write operation with a timestamp value and assumes 
that the latest write operation is the valid one. When you want to access a key/value pair, the partition owner retrieves all available copies for that pair
and compares the timestamp values. The latest one is the winner. If there is some outdated version of the requested pair, the primary owner propagates the latest
version of the pair. 

Read-repair is disabled by default for the sake of performance. If you have a use case that requires a more strict consistency control than a distributed caching 
scenario, you can enable read-repair via the configuration. 

#### Quorum-based replica control

Olric implements Read/Write quorum to keep the data in a consistent state. When you start a write operation on the cluster and write quorum (W) is 2, 
the partition owner tries to write the given key/value pair on its own data storage and on the replica nodes. If the number of successful write operations 
is below W, the primary owner returns `ErrWriteQuorum`. The read flow is the same: if you have R=2 and the owner only access one of the replicas, 
it returns `ErrReadQuorum`.

#### Simple Split-Brain Protection

Olric implements a technique called *majority quorum* to manage split-brain conditions. If a network partitioning occurs, and some members
lost the connection to rest of the cluster, they immediately stops functioning and return an error to incoming requests. This behaviour is controlled by
`MemberCountQuorum` parameter. It's default `1`. 

When the network healed, the stopped nodes joins again the cluster and fragmented partitions is merged by their primary owners in accordance with 
*LWW policy*. Olric also implements an *ownership report* mechanism to fix inconsistencies in partition distribution after a partitioning event. 

### Eviction
Olric supports different policies to evict keys from distributed maps. 

#### Expire with TTL
Olric implements TTL eviction policy. It shares the same algorithm with [Redis](https://redis.io/commands/expire#appendix-redis-expires):

> Periodically Redis tests a few keys at random among keys with an expire set. All the keys that are already expired are deleted from the keyspace.
>
> Specifically this is what Redis does 10 times per second:
>
> * Test 20 random keys from the set of keys with an associated expire.
> * Delete all the keys found expired.
> * If more than 25% of keys were expired, start again from step 1.
>
> This is a trivial probabilistic algorithm, basically the assumption is that our sample is representative of the whole key space, and we continue to expire until the percentage of keys that are likely to be expired is under 25%

When a client tries to access a key, Olric returns `ErrKeyNotFound` if the key is found to be timed out. A background task evicts keys with the algorithm described above.

#### Expire with MaxIdleDuration

Maximum time for each entry to stay idle in the DMap. It limits the lifetime of the entries relative to the time of the last read 
or write access performed on them. The entries whose idle period exceeds this limit are expired and evicted automatically. 
An entry is idle if no Get, Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration feature varies by 
preferred deployment method. 

#### Expire with LRU

Olric implements LRU eviction method on DMaps. Approximated LRU algorithm is borrowed from Redis. The Redis authors proposes the following algorithm:

> It is important to understand that the eviction process works like this:
> 
> * A client runs a new command, resulting in more data added.
> * Redis checks the memory usage, and if it is greater than the maxmemory limit , it evicts keys according to the policy.
> * A new command is executed, and so forth.
>
> So we continuously cross the boundaries of the memory limit, by going over it, and then by evicting keys to return back under the limits.
>
> If a command results in a lot of memory being used (like a big set intersection stored into a new key) for some time the memory 
> limit can be surpassed by a noticeable amount. 
>
> **Approximated LRU algorithm**
>
> Redis LRU algorithm is not an exact implementation. This means that Redis is not able to pick the best candidate for eviction, 
> that is, the access that was accessed the most in the past. Instead it will try to run an approximation of the LRU algorithm, 
> by sampling a small number of keys, and evicting the one that is the best (with the oldest access time) among the sampled keys.

Olric tracks access time for every DMap instance. Then it picks and sorts some configurable amount of keys to select keys for eviction.
Every node runs this algorithm independently. The access log is moved along with the partition when a network partition is occured.

#### Configuration of eviction mechanisms

Here is a simple configuration block for `olricd.yaml`: 

```
cache:
  numEvictionWorkers: 1
  maxIdleDuration: ""
  ttlDuration: "100s"
  maxKeys: 100000
  maxInuse: 1000000 # in bytes
  lRUSamples: 10
  evictionPolicy: "LRU" # NONE/LRU
```

You can also set cache configuration per DMap. Here is a simple configuration for a DMap named `foobar`:

```
dmaps:
  foobar:
    maxIdleDuration: "60s"
    ttlDuration: "300s"
    maxKeys: 500000 # in-bytes
    lRUSamples: 20
    evictionPolicy: "NONE" # NONE/LRU
```

If you prefer embedded-member deployment scenario, please take a look at [config#CacheConfig](https://godoc.org/github.com/buraksezer/olric/config#CacheConfig) and [config#DMapCacheConfig](https://godoc.org/github.com/buraksezer/olric/config#DMapCacheConfig) for the configuration.


### Lock Implementation

The DMap implementation is already thread-safe to meet your thread safety requirements. When you want to have more control on the
concurrency, you can use **LockWithTimeout** and **Lock** methods. Olric borrows the locking algorithm from Redis. Redis authors propose
the following algorithm:

> The command <SET resource-name anystring NX EX max-lock-time> is a simple way to implement a locking system with Redis.
>
> A client can acquire the lock if the above command returns OK (or retry after some time if the command returns Nil), and remove the lock just using DEL.
>
> The lock will be auto-released after the expire time is reached.
>
> It is possible to make this system more robust modifying the unlock schema as follows:
>
> Instead of setting a fixed string, set a non-guessable large random string, called token.
> Instead of releasing the lock with DEL, send a script that only removes the key if the value matches.
> This avoids that a client will try to release the lock after the expire time deleting the key created by another client that acquired the lock later.

Equivalent of`SETNX` command in Olric is `PutIf(key, value, IfNotFound)`. Lock and LockWithTimeout commands are properly implements
the algorithm which is proposed above. 

You should know that this implementation is subject to the clustering algorithm. So there is no guarantee about reliability in the case of network partitioning. I recommend the lock implementation to be used for 
efficiency purposes in general, instead of correctness.

**Important note about consistency:**

You should know that Olric is a PA/EC (see [Consistency and Replication Model](#consistency-and-replication-model)) product. So if your network is stable, all the operations on key/value 
pairs are performed by a single cluster member. It means that you can be sure about the consistency when the cluster is stable. It's important to know that computer networks fail 
occasionally, processes crash and random GC pauses may happen. Many factors can lead a network partitioning. If you cannot tolerate losing strong consistency under network partitioning, 
you need to use a different tool for locking.

See [Hazelcast and the Mythical PA/EC System](https://dbmsmusings.blogspot.com/2017/10/hazelcast-and-mythical-paec-system.html) and [Jepsen Analysis on Hazelcast 3.8.3](https://hazelcast.com/blog/jepsen-analysis-hazelcast-3-8-3/) for more insight on this topic.
             
### Storage Engine

Olric implements a GC-friendly storage engine to store large amounts of data on RAM. Basically, it applies an append-only log file approach with indexes. 
Olric inserts key/value pairs into pre-allocated byte slices (table in Olric terminology) and indexes that memory region by using Golang's built-in map. 
The data type of this map is `map[uint64]uint64`. When a pre-allocated byte slice is full Olric allocates a new one and continues inserting the new data into it. 
This design greatly reduces the write latency.

When you want to read a key/value pair from the Olric cluster, it scans the related DMap fragment by iterating over the indexes(implemented by the built-in map). 
The number of allocated byte slices should be small. So Olric would find the key immediately but technically, the read performance depends on the number of keys in the fragment. 
The effect of this design on the read performance is negligible.

The size of the pre-allocated byte slices is configurable.

## Samples

In this section, you can find code snippets for various scenarios.

### Embedded-member scenario
#### Distributed map
```go
package main

import (
  "context"
  "fmt"
  "log"
  "time"

  "github.com/buraksezer/olric"
  "github.com/buraksezer/olric/config"
)

func main() {
  // Sample for Olric v0.5.x

  // Deployment scenario: embedded-member
  // This creates a single-node Olric cluster. It's good enough for experimenting.

  // config.New returns a new config.Config with sane defaults. Available values for env:
  // local, lan, wan
  c := config.New("local")

  // Callback function. It's called when this node is ready to accept connections.
  ctx, cancel := context.WithCancel(context.Background())
  c.Started = func() {
    defer cancel()
    log.Println("[INFO] Olric is ready to accept connections")
  }

  // Create a new Olric instance.
  db, err := olric.New(c)
  if err != nil {
    log.Fatalf("Failed to create Olric instance: %v", err)
  }

  // Start the instance. It will form a single-node cluster.
  go func() {
    // Call Start at background. It's a blocker call.
    err = db.Start()
    if err != nil {
      log.Fatalf("olric.Start returned an error: %v", err)
    }
  }()

  <-ctx.Done()

  // In embedded-member scenario, you can use the EmbeddedClient. It implements
  // the Client interface.
  e := db.NewEmbeddedClient()

  dm, err := e.NewDMap("bucket-of-arbitrary-items")
  if err != nil {
    log.Fatalf("olric.NewDMap returned an error: %v", err)
  }

  ctx, cancel = context.WithCancel(context.Background())

  // Magic starts here!
  fmt.Println("##")
  fmt.Println("Simple Put/Get on a DMap instance:")
  err = dm.Put(ctx, "my-key", "Olric Rocks!")
  if err != nil {
    log.Fatalf("Failed to call Put: %v", err)
  }

  gr, err := dm.Get(ctx, "my-key")
  if err != nil {
    log.Fatalf("Failed to call Get: %v", err)
  }

  // Olric uses the Redis serialization format.
  value, err := gr.String()
  if err != nil {
    log.Fatalf("Failed to read Get response: %v", err)
  }

  fmt.Println("Response for my-key:", value)
  fmt.Println("##")

  // Don't forget the call Shutdown when you want to leave the cluster.
  ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()

  err = db.Shutdown(ctx)
  if err != nil {
    log.Printf("Failed to shutdown Olric: %v", err)
  }
}
```

#### Publish-Subscribe

```go
package main

import (
  "context"
  "fmt"
  "log"
  "time"

  "github.com/buraksezer/olric"
  "github.com/buraksezer/olric/config"
)

func main() {
  // Sample for Olric v0.5.x

  // Deployment scenario: embedded-member
  // This creates a single-node Olric cluster. It's good enough for experimenting.

  // config.New returns a new config.Config with sane defaults. Available values for env:
  // local, lan, wan
  c := config.New("local")

  // Callback function. It's called when this node is ready to accept connections.
  ctx, cancel := context.WithCancel(context.Background())
  c.Started = func() {
    defer cancel()
    log.Println("[INFO] Olric is ready to accept connections")
  }

  // Create a new Olric instance.
  db, err := olric.New(c)
  if err != nil {
    log.Fatalf("Failed to create Olric instance: %v", err)
  }

  // Start the instance. It will form a single-node cluster.
  go func() {
    // Call Start at background. It's a blocker call.
    err = db.Start()
    if err != nil {
      log.Fatalf("olric.Start returned an error: %v", err)
    }
  }()

  <-ctx.Done()

  // In embedded-member scenario, you can use the EmbeddedClient. It implements
  // the Client interface.
  e := db.NewEmbeddedClient()

  ps, err := e.NewPubSub()
  if err != nil {
    log.Fatalf("olric.NewPubSub returned an error: %v", err)
  }

  ctx, cancel = context.WithCancel(context.Background())

  // Olric implements a drop-in replacement of Redis Publish-Subscribe messaging
  // system. PubSub client is just a thin layer around go-redis/redis.
  rps := ps.Subscribe(ctx, "my-channel")

  // Get a message to read messages from my-channel
  msg := rps.Channel()

  go func() {
    // Publish a message here.
    _, err := ps.Publish(ctx, "my-channel", "Olric Rocks!")
    if err != nil {
      log.Fatalf("PubSub.Publish returned an error: %v", err)
    }
  }()

  // Consume messages
  rm := <-msg

  fmt.Printf("Received message: \"%s\" from \"%s\"", rm.Channel, rm.Payload)

  // Don't forget the call Shutdown when you want to leave the cluster.
  ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()

  err = e.Close(ctx)
  if err != nil {
    log.Printf("Failed to close EmbeddedClient: %v", err)
  }
}
```

### Client-Server scenario
#### Distributed map

```go
package main

import (
  "context"
  "fmt"
  "log"
  "time"

  "github.com/buraksezer/olric"
)

func main() {
  // Sample for Olric v0.5.x

  // Deployment scenario: client-server

  // NewClusterClient takes a list of the nodes. This list may only contain a
  // load balancer address. Please note that Olric nodes will calculate the partition owner
  // and proxy the incoming requests.
  c, err := olric.NewClusterClient([]string{"localhost:3320"})
  if err != nil {
    log.Fatalf("olric.NewClusterClient returned an error: %v", err)
  }

  // In client-server scenario, you can use the ClusterClient. It implements
  // the Client interface.
  dm, err := c.NewDMap("bucket-of-arbitrary-items")
  if err != nil {
    log.Fatalf("olric.NewDMap returned an error: %v", err)
  }

  ctx, cancel := context.WithCancel(context.Background())

  // Magic starts here!
  fmt.Println("##")
  fmt.Println("Simple Put/Get on a DMap instance:")
  err = dm.Put(ctx, "my-key", "Olric Rocks!")
  if err != nil {
    log.Fatalf("Failed to call Put: %v", err)
  }

  gr, err := dm.Get(ctx, "my-key")
  if err != nil {
    log.Fatalf("Failed to call Get: %v", err)
  }

  // Olric uses the Redis serialization format.
  value, err := gr.String()
  if err != nil {
    log.Fatalf("Failed to read Get response: %v", err)
  }

  fmt.Println("Response for my-key:", value)
  fmt.Println("##")

  // Don't forget the call Shutdown when you want to leave the cluster.
  ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()

  err = c.Close(ctx)
  if err != nil {
    log.Printf("Failed to close ClusterClient: %v", err)
  }
}
```

### SCAN on DMaps

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

func main() {
	// Sample for Olric v0.5.x

	// Deployment scenario: embedded-member
	// This creates a single-node Olric cluster. It's good enough for experimenting.

	// config.New returns a new config.Config with sane defaults. Available values for env:
	// local, lan, wan
	c := config.New("local")

	// Callback function. It's called when this node is ready to accept connections.
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}

	// Create a new Olric instance.
	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	// Start the instance. It will form a single-node cluster.
	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-ctx.Done()

	// In embedded-member scenario, you can use the EmbeddedClient. It implements
	// the Client interface.
	e := db.NewEmbeddedClient()

	dm, err := e.NewDMap("bucket-of-arbitrary-items")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())

	// Magic starts here!
	fmt.Println("##")
	fmt.Println("Insert 10 keys")
	var key string
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			key = fmt.Sprintf("even:%d", i)
		} else {
			key = fmt.Sprintf("odd:%d", i)
		}
		err = dm.Put(ctx, key, nil)
		if err != nil {
			log.Fatalf("Failed to call Put: %v", err)
		}
	}

	i, err := dm.Scan(ctx)
	if err != nil {
		log.Fatalf("Failed to call Scan: %v", err)
	}

	fmt.Println("Iterate over all the keys")
	for i.Next() {
		fmt.Println(">> Key", i.Key())
	}

	i.Close()

	i, err = dm.Scan(ctx, olric.Match("^even:"))
	if err != nil {
		log.Fatalf("Failed to call Scan: %v", err)
	}

	fmt.Println("\n\nScan with regex: ^even:")
	for i.Next() {
		fmt.Println(">> Key", i.Key())
	}

	i.Close()

	// Don't forget the call Shutdown when you want to leave the cluster.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.Shutdown(ctx)
	if err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
}
```

#### Publish-Subscribe
```go
package main

import (
  "context"
  "fmt"
  "log"
  "time"

  "github.com/buraksezer/olric"
)

func main() {
  // Sample for Olric v0.5.x

  // Deployment scenario: client-server

  // NewClusterClient takes a list of the nodes. This list may only contain a
  // load balancer address. Please note that Olric nodes will calculate the partition owner
  // and proxy the incoming requests.
  c, err := olric.NewClusterClient([]string{"localhost:3320"})
  if err != nil {
    log.Fatalf("olric.NewClusterClient returned an error: %v", err)
  }

  // In client-server scenario, you can use the ClusterClient. It implements
  // the Client interface.
  ps, err := c.NewPubSub()
  if err != nil {
    log.Fatalf("olric.NewPubSub returned an error: %v", err)
  }

  ctx, cancel := context.WithCancel(context.Background())

  // Olric implements a drop-in replacement of Redis Publish-Subscribe messaging
  // system. PubSub client is just a thin layer around go-redis/redis.
  rps := ps.Subscribe(ctx, "my-channel")

  // Get a message to read messages from my-channel
  msg := rps.Channel()

  go func() {
    // Publish a message here.
    _, err := ps.Publish(ctx, "my-channel", "Olric Rocks!")
    if err != nil {
      log.Fatalf("PubSub.Publish returned an error: %v", err)
    }
  }()

  // Consume messages
  rm := <-msg

  fmt.Printf("Received message: \"%s\" from \"%s\"", rm.Channel, rm.Payload)

  // Don't forget the call Shutdown when you want to leave the cluster.
  ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()

  err = c.Close(ctx)
  if err != nil {
    log.Printf("Failed to close ClusterClient: %v", err)
  }
}

```

## Contributions

Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

## License

The Apache License, Version 2.0 - see LICENSE for more details.

## About the name

The inner voice of Turgut Özben who is the main character of [Oğuz Atay's masterpiece -The Disconnected-](https://www.themodernnovel.org/asia/other-asia/turkey/oguz-atay/the-disconnected/).
