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

package protocol

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buraksezer/olric/internal/util"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

type Put struct {
	DMap  string
	Key   string
	Value []byte
	EX    float64
	PX    int64
	EXAT  float64
	PXAT  int64
	NX    bool
	XX    bool
}

func NewPut(dmap, key string, value []byte) *Put {
	return &Put{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (p *Put) SetEX(ex float64) *Put {
	p.EX = ex
	return p
}

func (p *Put) SetPX(px int64) *Put {
	p.PX = px
	return p
}

func (p *Put) SetEXAT(exat float64) *Put {
	p.EXAT = exat
	return p
}

func (p *Put) SetPXAT(pxat int64) *Put {
	p.PXAT = pxat
	return p
}

func (p *Put) SetNX() *Put {
	p.NX = true
	return p
}

func (p *Put) SetXX() *Put {
	p.XX = true
	return p
}

func (p *Put) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Put)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Value)

	if p.EX != 0 {
		args = append(args, "EX")
		args = append(args, p.EX)
	}

	if p.PX != 0 {
		args = append(args, "PX")
		args = append(args, p.PX)
	}

	if p.EXAT != 0 {
		args = append(args, "EXAT")
		args = append(args, p.EXAT)
	}

	if p.PXAT != 0 {
		args = append(args, "PXAT")
		args = append(args, p.PXAT)
	}

	if p.NX {
		args = append(args, "NX")
	}

	if p.XX {
		args = append(args, "XX")
	}

	return redis.NewStatusCmd(ctx, args...)
}

func ParsePutCommand(cmd redcon.Command) (*Put, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewPut(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		cmd.Args[3],                     // Value
	)

	args := cmd.Args[4:]
	for len(args) > 0 {
		switch arg := strings.ToUpper(util.BytesToString(args[0])); arg {
		case "NX":
			p.SetNX()
			args = args[1:]
			continue
		case "XX":
			p.SetXX()
			args = args[1:]
			continue
		case "PX":
			px, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPX(px)
			args = args[2:]
			continue
		case "EX":
			ex, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEX(ex)
			args = args[2:]
			continue
		case "EXAT":
			exat, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEXAT(exat)
			args = args[2:]
			continue
		case "PXAT":
			pxat, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPXAT(pxat)
			args = args[2:]
			continue
		default:
			return nil, errors.New("syntax error")
		}
	}

	return p, nil
}

type PutEntry struct {
	DMap  string
	Key   string
	Value []byte
}

func NewPutEntry(dmap, key string, value []byte) *PutEntry {
	return &PutEntry{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (p *PutEntry) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.PutEntry)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Value)
	return redis.NewStatusCmd(ctx, args...)
}

func ParsePutEntryCommand(cmd redcon.Command) (*PutEntry, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewPutEntry(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		cmd.Args[3],
	), nil
}

type Get struct {
	DMap string
	Key  string
	Raw  bool
}

func NewGet(dmap, key string) *Get {
	return &Get{
		DMap: dmap,
		Key:  key,
	}
}

func (g *Get) SetRaw() *Get {
	g.Raw = true
	return g
}

func (g *Get) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.Get)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	if g.Raw {
		args = append(args, "RW")
	}
	return redis.NewStringCmd(ctx, args...)
}

func ParseGetCommand(cmd redcon.Command) (*Get, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	g := NewGet(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
	)

	if len(cmd.Args) == 4 {
		arg := util.BytesToString(cmd.Args[3])
		if arg == "RW" {
			g.SetRaw()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return g, nil
}

type GetEntry struct {
	DMap    string
	Key     string
	Replica bool
}

func NewGetEntry(dmap, key string) *GetEntry {
	return &GetEntry{
		DMap: dmap,
		Key:  key,
	}
}

func (g *GetEntry) SetReplica() *GetEntry {
	g.Replica = true
	return g
}

func (g *GetEntry) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.GetEntry)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	if g.Replica {
		args = append(args, "RC")
	}
	return redis.NewStringCmd(ctx, args...)
}

func ParseGetEntryCommand(cmd redcon.Command) (*GetEntry, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	g := NewGetEntry(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
	)

	if len(cmd.Args) == 4 {
		arg := util.BytesToString(cmd.Args[3])
		if arg == "RC" {
			g.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return g, nil
}

type Del struct {
	DMap string
	Keys []string
}

func NewDel(dmap string, keys ...string) *Del {
	return &Del{
		DMap: dmap,
		Keys: keys,
	}
}

func (d *Del) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, DMap.Del)
	args = append(args, d.DMap)
	for _, key := range d.Keys {
		args = append(args, key)
	}
	return redis.NewIntCmd(ctx, args...)
}

func ParseDelCommand(cmd redcon.Command) (*Del, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	d := NewDel(
		util.BytesToString(cmd.Args[1]),
	)
	for _, key := range cmd.Args[2:] {
		d.Keys = append(d.Keys, util.BytesToString(key))
	}
	return d, nil
}

type DelEntry struct {
	Del     *Del
	Replica bool
}

func NewDelEntry(dmap, key string) *DelEntry {
	return &DelEntry{
		Del: NewDel(dmap, key),
	}
}

func (d *DelEntry) SetReplica() *DelEntry {
	d.Replica = true
	return d
}

func (d *DelEntry) Command(ctx context.Context) *redis.IntCmd {
	cmd := d.Del.Command(ctx)
	args := cmd.Args()
	args[0] = DMap.DelEntry
	if d.Replica {
		args = append(args, "RC")
	}
	return redis.NewIntCmd(ctx, args...)
}

func ParseDelEntryCommand(cmd redcon.Command) (*DelEntry, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}

	d := NewDelEntry(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
	)

	if len(cmd.Args) == 4 {
		arg := util.BytesToString(cmd.Args[3])
		if arg == "RC" {
			d.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return d, nil
}

type PExpire struct {
	DMap         string
	Key          string
	Milliseconds time.Duration
}

func NewPExpire(dmap, key string, milliseconds time.Duration) *PExpire {
	return &PExpire{
		DMap:         dmap,
		Key:          key,
		Milliseconds: milliseconds,
	}
}

func (p *PExpire) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.PExpire)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Milliseconds.Milliseconds())
	return redis.NewStatusCmd(ctx, args...)
}

func ParsePExpireCommand(cmd redcon.Command) (*PExpire, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	rawMilliseconds := util.BytesToString(cmd.Args[3])
	milliseconds, err := strconv.ParseInt(rawMilliseconds, 10, 64)
	if err != nil {
		return nil, err
	}
	p := NewPExpire(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		time.Duration(milliseconds*int64(time.Millisecond)),
	)
	return p, nil
}

type Expire struct {
	DMap    string
	Key     string
	Seconds time.Duration
}

func NewExpire(dmap, key string, seconds time.Duration) *Expire {
	return &Expire{
		DMap:    dmap,
		Key:     key,
		Seconds: seconds,
	}
}

func (e *Expire) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Expire)
	args = append(args, e.DMap)
	args = append(args, e.Key)
	args = append(args, e.Seconds.Seconds())
	return redis.NewStatusCmd(ctx, args...)
}

func ParseExpireCommand(cmd redcon.Command) (*Expire, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	rawSeconds := util.BytesToString(cmd.Args[3])
	seconds, err := strconv.ParseFloat(rawSeconds, 64)
	if err != nil {
		return nil, err
	}
	e := NewExpire(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		time.Duration(seconds*float64(time.Second)),
	)
	return e, nil
}

type Destroy struct {
	DMap  string
	Local bool
}

func NewDestroy(dmap string) *Destroy {
	return &Destroy{
		DMap: dmap,
	}
}

func (d *Destroy) SetLocal() *Destroy {
	d.Local = true
	return d
}

func (d *Destroy) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Destroy)
	args = append(args, d.DMap)
	if d.Local {
		args = append(args, "LC")
	}
	return redis.NewStatusCmd(ctx, args...)
}

func ParseDestroyCommand(cmd redcon.Command) (*Destroy, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	d := NewDestroy(
		util.BytesToString(cmd.Args[1]),
	)

	if len(cmd.Args) == 3 {
		arg := util.BytesToString(cmd.Args[2])
		if arg == "LC" {
			d.SetLocal()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return d, nil
}

type Scan struct {
	PartID  uint64
	DMap    string
	Cursor  uint64
	Count   int
	Match   string
	Replica bool
}

func NewScan(partID uint64, dmap string, cursor uint64) *Scan {
	return &Scan{
		PartID: partID,
		DMap:   dmap,
		Cursor: cursor,
	}
}

func (s *Scan) SetMatch(match string) *Scan {
	s.Match = match
	return s
}

func (s *Scan) SetCount(count int) *Scan {
	s.Count = count
	return s
}

func (s *Scan) SetReplica() *Scan {
	s.Replica = true
	return s
}

func (s *Scan) Command(ctx context.Context) *redis.ScanCmd {
	var args []interface{}
	args = append(args, DMap.Scan)
	args = append(args, s.PartID)
	args = append(args, s.DMap)
	args = append(args, s.Cursor)
	if s.Match != "" {
		args = append(args, "MATCH")
		args = append(args, s.Match)
	}
	if s.Count != 0 {
		args = append(args, "COUNT")
		args = append(args, s.Count)
	}
	if s.Replica {
		args = append(args, "RC")
	}
	return redis.NewScanCmd(ctx, nil, args...)
}

const DefaultScanCount = 10

func ParseScanCommand(cmd redcon.Command) (*Scan, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	rawPartID := util.BytesToString(cmd.Args[1])
	partID, err := strconv.ParseUint(rawPartID, 10, 64)
	if err != nil {
		return nil, err
	}

	rawCursor := util.BytesToString(cmd.Args[3])
	cursor, err := strconv.ParseUint(rawCursor, 10, 64)
	if err != nil {
		return nil, err
	}

	s := NewScan(
		partID,
		util.BytesToString(cmd.Args[2]), // DMap
		cursor,
	)

	args := cmd.Args[4:]
	for len(args) > 0 {
		switch arg := strings.ToUpper(util.BytesToString(args[0])); arg {
		case "MATCH":
			s.SetMatch(util.BytesToString(args[1]))
			args = args[2:]
			continue
		case "COUNT":
			count, err := strconv.Atoi(util.BytesToString(args[1]))
			if err != nil {
				return nil, err
			}
			s.SetCount(count)
			args = args[2:]
			continue
		case "RC":
			s.SetReplica()
			args = args[1:]
		}
	}

	if s.Count == 0 {
		s.SetCount(DefaultScanCount)
	}

	return s, nil
}

type Incr struct {
	DMap  string
	Key   string
	Delta int
}

func NewIncr(dmap, key string, delta int) *Incr {
	return &Incr{
		DMap:  dmap,
		Key:   key,
		Delta: delta,
	}
}

func (i *Incr) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, DMap.Incr)
	args = append(args, i.DMap)
	args = append(args, i.Key)
	args = append(args, i.Delta)
	return redis.NewIntCmd(ctx, args...)
}

func ParseIncrCommand(cmd redcon.Command) (*Incr, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	delta, err := strconv.Atoi(util.BytesToString(cmd.Args[3]))
	if err != nil {
		return nil, err
	}

	return NewIncr(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		delta,
	), nil
}

type Decr struct {
	*Incr
}

func NewDecr(dmap, key string, delta int) *Decr {
	return &Decr{
		NewIncr(dmap, key, delta),
	}
}

func (d *Decr) Command(ctx context.Context) *redis.IntCmd {
	cmd := d.Incr.Command(ctx)
	cmd.Args()[0] = DMap.Decr
	return cmd
}

func ParseDecrCommand(cmd redcon.Command) (*Decr, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	delta, err := strconv.Atoi(util.BytesToString(cmd.Args[3]))
	if err != nil {
		return nil, err
	}

	return NewDecr(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		delta,
	), nil
}

type GetPut struct {
	DMap  string
	Key   string
	Value []byte
	Raw   bool
}

func NewGetPut(dmap, key string, value []byte) *GetPut {
	return &GetPut{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (g *GetPut) SetRaw() *GetPut {
	g.Raw = true
	return g
}

func (g *GetPut) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.GetPut)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	args = append(args, g.Value)
	if g.Raw {
		args = append(args, "RW")
	}
	return redis.NewStringCmd(ctx, args...)
}

func ParseGetPutCommand(cmd redcon.Command) (*GetPut, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	g := NewGetPut(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		cmd.Args[3],                     // Value
	)

	if len(cmd.Args) == 5 {
		arg := util.BytesToString(cmd.Args[4])
		if arg == "RW" {
			g.SetRaw()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}
	return g, nil
}

type IncrByFloat struct {
	DMap  string
	Key   string
	Delta float64
}

func NewIncrByFloat(dmap, key string, delta float64) *IncrByFloat {
	return &IncrByFloat{
		DMap:  dmap,
		Key:   key,
		Delta: delta,
	}
}

func (i *IncrByFloat) Command(ctx context.Context) *redis.FloatCmd {
	var args []interface{}
	args = append(args, DMap.IncrByFloat)
	args = append(args, i.DMap)
	args = append(args, i.Key)
	args = append(args, i.Delta)
	return redis.NewFloatCmd(ctx, args...)
}

func ParseIncrByFloatCommand(cmd redcon.Command) (*IncrByFloat, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	delta, err := strconv.ParseFloat(util.BytesToString(cmd.Args[3]), 10)
	if err != nil {
		return nil, err
	}

	return NewIncrByFloat(
		util.BytesToString(cmd.Args[1]),
		util.BytesToString(cmd.Args[2]),
		delta,
	), nil
}

type Lock struct {
	DMap     string
	Key      string
	Deadline float64
	EX       float64
	PX       int64
}

func NewLock(dmap, key string, deadline float64) *Lock {
	return &Lock{
		DMap:     dmap,
		Key:      key,
		Deadline: deadline,
	}
}

func (l *Lock) SetEX(ex float64) *Lock {
	l.EX = ex
	return l
}

func (l *Lock) SetPX(px int64) *Lock {
	l.PX = px
	return l
}

func (l *Lock) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.Lock)
	args = append(args, l.DMap)
	args = append(args, l.Key)
	args = append(args, l.Deadline)

	// Options
	if l.EX != 0 {
		args = append(args, "EX")
		args = append(args, l.EX)
	}

	if l.PX != 0 {
		args = append(args, "PX")
		args = append(args, l.PX)
	}

	return redis.NewStringCmd(ctx, args...)
}

func ParseLockCommand(cmd redcon.Command) (*Lock, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	deadline, err := strconv.ParseFloat(util.BytesToString(cmd.Args[3]), 64)
	if err != nil {
		return nil, err
	}

	l := NewLock(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		deadline,                        // Deadline
	)

	// EX or PX are optional.
	if len(cmd.Args) > 4 {
		if len(cmd.Args) == 5 {
			return nil, fmt.Errorf("%w: %s needs a numerical argument", ErrInvalidArgument, util.BytesToString(cmd.Args[5]))
		}

		switch arg := strings.ToUpper(util.BytesToString(cmd.Args[4])); arg {
		case "PX":
			px, err := strconv.ParseInt(util.BytesToString(cmd.Args[5]), 10, 64)
			if err != nil {
				return nil, err
			}
			l.PX = px
		case "EX":
			ex, err := strconv.ParseFloat(util.BytesToString(cmd.Args[5]), 64)
			if err != nil {
				return nil, err
			}
			l.EX = ex
		default:
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return l, nil
}

type Unlock struct {
	DMap  string
	Key   string
	Token string
}

func NewUnlock(dmap, key, token string) *Unlock {
	return &Unlock{
		DMap:  dmap,
		Key:   key,
		Token: token,
	}
}

func (u *Unlock) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Unlock)
	args = append(args, u.DMap)
	args = append(args, u.Key)
	args = append(args, u.Token)
	return redis.NewStatusCmd(ctx, args...)
}

func ParseUnlockCommand(cmd redcon.Command) (*Unlock, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewUnlock(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		util.BytesToString(cmd.Args[3]), // Token
	), nil
}

type LockLease struct {
	DMap    string
	Key     string
	Token   string
	Timeout float64
}

func NewLockLease(dmap, key, token string, timeout float64) *LockLease {
	return &LockLease{
		DMap:    dmap,
		Key:     key,
		Token:   token,
		Timeout: timeout,
	}
}

func (l *LockLease) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.LockLease)
	args = append(args, l.DMap)
	args = append(args, l.Key)
	args = append(args, l.Token)
	args = append(args, l.Timeout)
	return redis.NewStatusCmd(ctx, args...)
}

func ParseLockLeaseCommand(cmd redcon.Command) (*LockLease, error) {
	if len(cmd.Args) < 5 {
		return nil, errWrongNumber(cmd.Args)
	}

	timeout, err := strconv.ParseFloat(util.BytesToString(cmd.Args[4]), 64)
	if err != nil {
		return nil, err
	}

	return NewLockLease(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		util.BytesToString(cmd.Args[3]), // Token
		timeout,                         // Timeout
	), nil
}

type PLockLease struct {
	DMap    string
	Key     string
	Token   string
	Timeout int64
}

func NewPLockLease(dmap, key, token string, timeout int64) *PLockLease {
	return &PLockLease{
		DMap:    dmap,
		Key:     key,
		Token:   token,
		Timeout: timeout,
	}
}

func (p *PLockLease) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.PLockLease)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Token)
	args = append(args, p.Timeout)
	return redis.NewStatusCmd(ctx, args...)
}

func ParsePLockLeaseCommand(cmd redcon.Command) (*PLockLease, error) {
	if len(cmd.Args) < 5 {
		return nil, errWrongNumber(cmd.Args)
	}

	timeout, err := strconv.ParseInt(util.BytesToString(cmd.Args[4]), 10, 64)
	if err != nil {
		return nil, err
	}

	return NewPLockLease(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		util.BytesToString(cmd.Args[3]), // Token
		timeout,                         // Timeout
	), nil
}
