// Copyright 2018-2024 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package olric

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var (
	// ErrNotReady denotes that the Future instance you hold is not ready to read the response yet.
	ErrNotReady = errors.New("not ready yet")

	// ErrPipelineClosed denotes that the underlying pipeline is closed, and it's impossible to operate.
	ErrPipelineClosed = errors.New("pipeline is closed")

	// ErrPipelineExecuted denotes that Exec was already called on the underlying pipeline.
	ErrPipelineExecuted = errors.New("pipeline already executed")
)

// DMapPipeline implements a pipeline for the following methods of the DMap API:
//
// * Put
// * Get
// * Delete
// * Incr
// * Decr
// * GetPut
// * IncrByFloat
//
// DMapPipeline enables batch operations on DMap data.
type DMapPipeline struct {
	mtx          sync.Mutex
	dm           *ClusterDMap
	commands     map[uint64][]redis.Cmder
	result       map[uint64][]redis.Cmder
	ctx          context.Context
	cancel       context.CancelFunc
	closedCtx    context.Context // used to detect if the pipeline is closed / discarded
	closedCancel context.CancelFunc

	concurrency int // defaults to runtime.NumCPU()
}

func (dp *DMapPipeline) addCommand(key string, cmd redis.Cmder) (uint64, int) {
	dp.mtx.Lock()
	defer dp.mtx.Unlock()

	hkey := partitions.HKey(dp.dm.name, key)
	partID := hkey % dp.dm.clusterClient.partitionCount

	cmds, ok := dp.commands[partID]
	if !ok {
		// if there are no existing commands, get a new slice from the pool
		cmds = getPipelineCmdsFromPool()
	}
	dp.commands[partID] = append(cmds, cmd)

	return partID, len(dp.commands[partID]) - 1
}

// FuturePut is used to read the result of a pipelined Put command.
type FuturePut struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined Put command.
func (f *FuturePut) Result() error {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		return processProtocolError(cmd.Err())
	default:
		return ErrNotReady
	}
}

// Put queues a Put command. The parameters are identical to the DMap.Put,
// but it returns FuturePut to read the batched response.
func (dp *DMapPipeline) Put(ctx context.Context, key string, value interface{}, options ...PutOption) (*FuturePut, error) {
	buf := bytes.NewBuffer(nil)

	enc := resp.New(buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}

	var pc dmap.PutConfig
	for _, opt := range options {
		opt(&pc)
	}

	cmd := dp.dm.writePutCommand(&pc, key, buf.Bytes()).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FuturePut{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}, nil
}

// FutureGet is used to read result of a pipelined Get command.
type FutureGet struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined Get command.
func (f *FutureGet) Result() (*GetResponse, error) {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return nil, ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return nil, processProtocolError(cmd.Err())
		}
		stringCmd := redis.NewStringCmd(context.Background(), cmd.Args()...)
		stringCmd.SetVal(cmd.(*redis.Cmd).Val().(string))
		return f.dp.dm.makeGetResponse(stringCmd)
	default:
		return nil, ErrNotReady
	}
}

// Get queues a Get command. The parameters are identical to the DMap.Get,
// but it returns FutureGet to read the batched response.
func (dp *DMapPipeline) Get(ctx context.Context, key string) *FutureGet {
	cmd := protocol.NewGet(dp.dm.name, key).SetRaw().Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureGet{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}
}

// FutureDelete is used to read the result of a pipelined Delete command.
type FutureDelete struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined Delete command.
func (f *FutureDelete) Result() (int, error) {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return 0, ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		return int(cmd.(*redis.Cmd).Val().(int64)), nil
	default:
		return 0, ErrNotReady
	}
}

// Delete queues a Delete command. The parameters are identical to the DMap.Delete,
// but it returns FutureDelete to read the batched response.
func (dp *DMapPipeline) Delete(ctx context.Context, key string) *FutureDelete {
	cmd := protocol.NewDel(dp.dm.name, []string{key}...).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureDelete{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}
}

// FutureExpire is used to read the result of a pipelined Expire command.
type FutureExpire struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined Expire command.
func (f *FutureExpire) Result() error {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		return processProtocolError(cmd.Err())
	default:
		return ErrNotReady
	}
}

// Expire queues an Expire command. The parameters are identical to the DMap.Expire,
// but it returns FutureExpire to read the batched response.
func (dp *DMapPipeline) Expire(ctx context.Context, key string, timeout time.Duration) (*FutureExpire, error) {
	cmd := protocol.NewExpire(dp.dm.name, key, timeout).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureExpire{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}, nil
}

// FutureIncr is used to read the result of a pipelined Incr command.
type FutureIncr struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined Incr command.
func (f *FutureIncr) Result() (int, error) {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return 0, ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		return int(cmd.(*redis.Cmd).Val().(int64)), nil
	default:
		return 0, ErrNotReady
	}
}

// Incr queues an Incr command. The parameters are identical to the DMap.Incr,
// but it returns FutureIncr to read the batched response.
func (dp *DMapPipeline) Incr(ctx context.Context, key string, delta int) (*FutureIncr, error) {
	cmd := protocol.NewIncr(dp.dm.name, key, delta).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureIncr{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}, nil
}

// FutureDecr is used to read the result of a pipelined Decr command.
type FutureDecr struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined Decr command.
func (f *FutureDecr) Result() (int, error) {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return 0, ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		return int(cmd.(*redis.Cmd).Val().(int64)), nil
	default:
		return 0, ErrNotReady
	}
}

// Decr queues a Decr command. The parameters are identical to the DMap.Decr,
// but it returns FutureDecr to read the batched response.
func (dp *DMapPipeline) Decr(ctx context.Context, key string, delta int) (*FutureDecr, error) {
	cmd := protocol.NewDecr(dp.dm.name, key, delta).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureDecr{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}, nil
}

// FutureGetPut is used to read the result of a pipelined GetPut command.
type FutureGetPut struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined GetPut command.
func (f *FutureGetPut) Result() (*GetResponse, error) {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return nil, ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() == redis.Nil {
			// This should be the first run.
			return nil, nil
		}
		if cmd.Err() != nil {
			return nil, processProtocolError(cmd.Err())
		}
		stringCmd := redis.NewStringCmd(context.Background(), cmd.Args()...)
		stringCmd.SetVal(cmd.(*redis.Cmd).Val().(string))
		return f.dp.dm.makeGetResponse(stringCmd)
	default:
		return nil, ErrNotReady
	}
}

// GetPut queues a GetPut command. The parameters are identical to the DMap.GetPut,
// but it returns FutureGetPut to read the batched response.
func (dp *DMapPipeline) GetPut(ctx context.Context, key string, value interface{}) (*FutureGetPut, error) {
	buf := bytes.NewBuffer(nil)

	enc := resp.New(buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}

	cmd := protocol.NewGetPut(dp.dm.name, key, buf.Bytes()).SetRaw().Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureGetPut{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}, nil
}

// FutureIncrByFloat is used to read the result of a pipelined IncrByFloat command.
type FutureIncrByFloat struct {
	dp        *DMapPipeline
	partID    uint64
	index     int
	ctx       context.Context
	closedCtx context.Context
}

// Result returns a response for the pipelined IncrByFloat command.
func (f *FutureIncrByFloat) Result() (float64, error) {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-f.closedCtx.Done():
		return 0, ErrPipelineClosed
	default:
	}

	select {
	case <-f.ctx.Done():
		cmd := f.dp.result[f.partID][f.index]
		if cmd.Err() != nil {
			return 0, processProtocolError(cmd.Err())
		}
		stringRes := cmd.(*redis.Cmd).Val().(string)
		return strconv.ParseFloat(stringRes, 64)
	default:
		return 0, ErrNotReady
	}
}

// IncrByFloat queues an IncrByFloat command. The parameters are identical to the DMap.IncrByFloat,
// but it returns FutureIncrByFloat to read the batched response.
func (dp *DMapPipeline) IncrByFloat(ctx context.Context, key string, delta float64) (*FutureIncrByFloat, error) {
	cmd := protocol.NewIncrByFloat(dp.dm.name, key, delta).Command(ctx)
	partID, index := dp.addCommand(key, cmd)
	return &FutureIncrByFloat{
		dp:        dp,
		partID:    partID,
		index:     index,
		ctx:       dp.ctx,
		closedCtx: dp.closedCtx,
	}, nil
}

func (dp *DMapPipeline) execOnPartition(ctx context.Context, partID uint64) error {
	rc, err := dp.dm.clusterClient.clientByPartID(partID)
	if err != nil {
		return err
	}
	// There is no need to protect dp.commands map and its content.
	// It's already filled before running Exec, and it's now a read-only
	// data structure
	commands := dp.commands[partID]
	pipe := rc.Pipeline()

	for _, cmd := range commands {
		pipe.Do(ctx, cmd.Args()...)
	}

	// Exec executes all previously queued commands using one
	// client-server roundtrip.
	//
	// Exec always returns list of commands and error of the first failed
	// command if any.
	result, _ := pipe.Exec(ctx)
	dp.mtx.Lock()
	dp.result[partID] = result
	dp.mtx.Unlock()
	return nil
}

// Exec executes all queued commands using one client-server roundtrip per partition.
func (dp *DMapPipeline) Exec(ctx context.Context) error {
	// this select is separate from the one below on purpose, since select is non-deterministic if multiple
	// cases are available, and we need to guarantee this check first.
	select {
	case <-dp.closedCtx.Done():
		return ErrPipelineClosed
	default:
	}

	// this checks to see if Exec has already run. While Exec should only be called once, it is possible that
	// the user could call Exec multiple times. If we stored the result of errGr.Wait on the pipeline, we could
	// return that error and make Exec idempotent.
	select {
	case <-dp.ctx.Done():
		return ErrPipelineExecuted
	default:
	}

	defer dp.cancel()

	var errGr errgroup.Group
	sem := semaphore.NewWeighted(int64(dp.concurrency))
	for i := uint64(0); i < dp.dm.clusterClient.partitionCount; i++ {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		partID := i
		errGr.Go(func() error {
			defer sem.Release(1)
			// If execOnPartition returns an error, it will eventually stop
			// all flush operation.
			return dp.execOnPartition(ctx, partID)
		})
	}

	return errGr.Wait()
}

// Discard discards the pipelined commands and resets all internal states.
// A pipeline can be reused after calling Discard.
func (dp *DMapPipeline) Discard() error {
	select {
	case <-dp.closedCtx.Done():
		return ErrPipelineClosed
	default:
	}

	dp.closedCancel()

	dp.mtx.Lock()
	defer dp.mtx.Unlock()

	// return all command slices to the pool

	for _, v := range dp.commands {
		putPipelineCmdsIntoPool(v)
	}

	for _, v := range dp.result {
		putPipelineCmdsIntoPool(v)
	}

	// the deletes below are purposefully not combined with the loops above, as these are recognized and optimized
	// by the compiler. https://go-review.googlesource.com/c/go/+/110055

	for k := range dp.commands {
		delete(dp.commands, k)
	}

	for k := range dp.result {
		delete(dp.result, k)
	}

	dp.initContexts()

	return nil
}

// Close closes the pipeline and frees the allocated resources. You shouldn't try to
// reuse a closed pipeline.
func (dp *DMapPipeline) Close() {
	dp.closedCancel()
}

// Pipeline is a mechanism to realise Redis Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Redis and read a replies in a
// singe step.
// See https://redis.io/topics/pipelining
//
// Pay attention, that Pipeline is not a transaction, so you can get unexpected
// results in case of big pipelines and small read/write timeouts.
// Redis client has retransmission logic in case of timeouts, pipeline
// can be retransmitted and commands can be executed more than once.
func (dm *ClusterDMap) Pipeline(opts ...PipelineOption) (*DMapPipeline, error) {
	dp := &DMapPipeline{
		dm:       dm,
		commands: make(map[uint64][]redis.Cmder),
		result:   make(map[uint64][]redis.Cmder),

		concurrency: runtime.NumCPU(),
	}

	for _, opt := range opts {
		opt(dp)
	}

	dp.initContexts()

	return dp, nil
}

// initContexts sets up chained contexts for the pipeline. The base is closedCtx, which is closed either in
// Close or Discard. ctx is a child of closedCtx, as we want to cancel the pipeline if it is closed. It is
// canceled in Exec, and used to block FutureXXX.Result() calls until Exec has completed.
func (dp *DMapPipeline) initContexts() {
	dp.closedCtx, dp.closedCancel = context.WithCancel(context.Background())
	dp.ctx, dp.cancel = context.WithCancel(dp.closedCtx)
}

// This stores a slice of commands for each partition. There is a possibility that a single
// large slice could be allocated with an unusually large number of commands in a single pipeline that
// are very unbalanced across partitions, but that is unlikely to be a problem in practice.
//
// It does not store a pointer to the slice as recommended by staticcheck because that is harder to reason
// about, and a single allocation is not a big deal compared to the slices we're able to reuse.
// https://staticcheck.io/docs/checks#SA6002
// https://github.com/dominikh/go-tools/issues/1336#issuecomment-1331206290
var pipelineCmdPool = sync.Pool{
	New: func() interface{} {
		return make([]redis.Cmder, 0)
	},
}

func getPipelineCmdsFromPool() []redis.Cmder {
	return pipelineCmdPool.Get().([]redis.Cmder)
}

func putPipelineCmdsIntoPool(cmds []redis.Cmder) {
	// remove references to underlying commands so they can be GCed
	for i := range cmds {
		cmds[i] = nil
	}
	cmds = cmds[:0]
	pipelineCmdPool.Put(cmds)
}
