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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func stringToCommand(s string) redcon.Command {
	cmd := redcon.Command{
		Raw: []byte(s),
	}

	s = strings.TrimSuffix(s, ": []")
	s = strings.TrimSuffix(s, ": 0")
	s = strings.TrimSuffix(s, ":")
	s = strings.TrimSuffix(s, ": ")
	parsed := strings.Split(s, " ")
	for _, arg := range parsed {
		cmd.Args = append(cmd.Args, []byte(arg))
	}
	return cmd
}

func TestProtocol_ParsePutCommand_EX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetEX((10 * time.Second).Seconds())

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, float64(10), parsed.EX)
}

func TestProtocol_ParsePutCommand_PX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetPX((100 * time.Millisecond).Milliseconds())

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, int64(100), parsed.PX)
}

func TestProtocol_ParsePutCommand_NX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetNX()

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.True(t, parsed.NX)
	require.False(t, parsed.XX)
}

func TestProtocol_ParsePutCommand_XX(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	putCmd.SetXX()

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.True(t, parsed.XX)
	require.False(t, parsed.NX)
}

func TestProtocol_ParsePutCommand_EXAT(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	exat := float64(time.Now().Unix()) + 10
	putCmd.SetEXAT(exat)

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, exat, parsed.EXAT)
}

func TestProtocol_ParsePutCommand_PXAT(t *testing.T) {
	putCmd := NewPut("my-dmap", "my-key", []byte("my-value"))
	pxat := (time.Now().UnixNano() / 1000000) + 10
	putCmd.SetPXAT(pxat)

	cmd := stringToCommand(putCmd.Command(context.Background()).String())
	parsed, err := ParsePutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.Equal(t, pxat, parsed.PXAT)
}

func TestProtocol_ParseScanCommand(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0)

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "", parsed.Match)
	require.Equal(t, 10, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Replica(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetReplica()

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "", parsed.Match)
	require.Equal(t, 10, parsed.Count)
	require.True(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Match(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetMatch("^even")

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "^even", parsed.Match)
	require.Equal(t, 10, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_PartID(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetCount(200)

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "", parsed.Match)
	require.Equal(t, 200, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Match_Count(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).SetCount(100).SetMatch("^even")

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "^even", parsed.Match)
	require.Equal(t, 100, parsed.Count)
	require.False(t, scanCmd.Replica)
}

func TestProtocol_ParseScanCommand_Match_Count_Replica(t *testing.T) {
	scanCmd := NewScan(1, "my-dmap", 0).
		SetCount(100).
		SetMatch("^even").
		SetReplica()

	s := scanCmd.Command(context.Background()).String()
	s = strings.TrimSuffix(s, ": []")
	cmd := stringToCommand(s)
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(1), parsed.PartID)
	require.Equal(t, "^even", parsed.Match)
	require.Equal(t, 100, parsed.Count)
	require.True(t, scanCmd.Replica)
}

func TestProtocol_PutEntry(t *testing.T) {
	putEntryCmd := NewPutEntry("my-dmap", "my-key", []byte("my-value"))

	cmd := stringToCommand(putEntryCmd.Command(context.Background()).String())
	parsed, err := ParsePutEntryCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
}

func TestProtocol_Get(t *testing.T) {
	getCmd := NewGet("my-dmap", "my-key")

	cmd := stringToCommand(getCmd.Command(context.Background()).String())
	parsed, err := ParseGetCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.False(t, parsed.Raw)
}

func TestProtocol_Get_RW(t *testing.T) {
	getCmd := NewGet("my-dmap", "my-key")
	getCmd.SetRaw()

	cmd := stringToCommand(getCmd.Command(context.Background()).String())
	parsed, err := ParseGetCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.True(t, parsed.Raw)
}

func TestProtocol_GetEntry(t *testing.T) {
	getEntryCmd := NewGetEntry("my-dmap", "my-key")

	cmd := stringToCommand(getEntryCmd.Command(context.Background()).String())
	parsed, err := ParseGetEntryCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.False(t, parsed.Replica)
}

func TestProtocol_GetEntry_RC(t *testing.T) {
	getEntryCmd := NewGetEntry("my-dmap", "my-key")
	getEntryCmd.SetReplica()

	cmd := stringToCommand(getEntryCmd.Command(context.Background()).String())
	parsed, err := ParseGetEntryCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.True(t, parsed.Replica)
}

func TestProtocol_Del(t *testing.T) {
	delCmd := NewDel("my-dmap", "key1", "key2")

	cmd := stringToCommand(delCmd.Command(context.Background()).String())
	parsed, err := ParseDelCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, []string{"key1", "key2"}, parsed.Keys)
}

func TestProtocol_DelEntry(t *testing.T) {
	delEntryCmd := NewDelEntry("my-dmap", "my-key")

	cmd := stringToCommand(delEntryCmd.Command(context.Background()).String())
	parsed, err := ParseDelEntryCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.Del.DMap)
	require.Equal(t, []string{"my-key"}, parsed.Del.Keys)
	require.False(t, parsed.Replica)
}

func TestProtocol_DelEntry_RC(t *testing.T) {
	delEntryCmd := NewDelEntry("my-dmap", "my-key")
	delEntryCmd.SetReplica()

	cmd := stringToCommand(delEntryCmd.Command(context.Background()).String())
	parsed, err := ParseDelEntryCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.Del.DMap)
	require.Equal(t, []string{"my-key"}, parsed.Del.Keys)
	require.True(t, parsed.Replica)
}

func TestProtocol_PExpire(t *testing.T) {
	pexpireCmd := NewPExpire("my-dmap", "my-key", 10*time.Millisecond)

	cmd := stringToCommand(pexpireCmd.Command(context.Background()).String())
	parsed, err := ParsePExpireCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, 10*time.Millisecond, parsed.Milliseconds)
}

func TestProtocol_Expire(t *testing.T) {
	pexpireCmd := NewExpire("my-dmap", "my-key", 10*time.Second)

	cmd := stringToCommand(pexpireCmd.Command(context.Background()).String())
	parsed, err := ParseExpireCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, 10*time.Second, parsed.Seconds)
}

func TestProtocol_Destroy(t *testing.T) {
	destroyCmd := NewDestroy("my-dmap")

	cmd := stringToCommand(destroyCmd.Command(context.Background()).String())
	parsed, err := ParseDestroyCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.False(t, parsed.Local)
}

func TestProtocol_Destroy_Local(t *testing.T) {
	destroyCmd := NewDestroy("my-dmap")
	destroyCmd.SetLocal()

	cmd := stringToCommand(destroyCmd.Command(context.Background()).String())
	parsed, err := ParseDestroyCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.True(t, parsed.Local)
}

func TestProtocol_Incr(t *testing.T) {
	incrCmd := NewIncr("my-dmap", "my-key", 7)

	cmd := stringToCommand(incrCmd.Command(context.Background()).String())
	parsed, err := ParseIncrCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, 7, parsed.Delta)
}

func TestProtocol_Decr(t *testing.T) {
	decrCmd := NewDecr("my-dmap", "my-key", 7)

	cmd := stringToCommand(decrCmd.Command(context.Background()).String())
	parsed, err := ParseDecrCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, 7, parsed.Delta)
}

func TestProtocol_GetPut(t *testing.T) {
	getputCmd := NewGetPut("my-dmap", "my-key", []byte("my-value"))

	cmd := stringToCommand(getputCmd.Command(context.Background()).String())
	parsed, err := ParseGetPutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.False(t, parsed.Raw)
}

func TestProtocol_GetPut_RW(t *testing.T) {
	getputCmd := NewGetPut("my-dmap", "my-key", []byte("my-value"))
	getputCmd.SetRaw()

	cmd := stringToCommand(getputCmd.Command(context.Background()).String())
	parsed, err := ParseGetPutCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, []byte("my-value"), parsed.Value)
	require.True(t, parsed.Raw)
}

func TestProtocol_IncrByFloat(t *testing.T) {
	incrByFloatCmd := NewIncrByFloat("my-dmap", "my-key", 3.14159265359)

	cmd := stringToCommand(incrByFloatCmd.Command(context.Background()).String())
	parsed, err := ParseIncrByFloatCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, 3.14159265359, parsed.Delta)
}

func TestProtocol_Lock(t *testing.T) {
	lockCmd := NewLock("my-dmap", "my-key", 7)

	cmd := stringToCommand(lockCmd.Command(context.Background()).String())
	parsed, err := ParseLockCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, float64(7), parsed.Deadline)
}

func TestProtocol_Lock_EX(t *testing.T) {
	exDuration := (250 * time.Second).Seconds()
	lockCmd := NewLock("my-dmap", "my-key", 7)
	lockCmd.SetEX(exDuration)

	cmd := stringToCommand(lockCmd.Command(context.Background()).String())
	parsed, err := ParseLockCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, float64(7), parsed.Deadline)
	require.Equal(t, exDuration, parsed.EX)
}

func TestProtocol_Lock_PX(t *testing.T) {
	pxDuration := (250 * time.Millisecond).Milliseconds()
	lockCmd := NewLock("my-dmap", "my-key", 7)
	lockCmd.SetPX(pxDuration)

	cmd := stringToCommand(lockCmd.Command(context.Background()).String())
	parsed, err := ParseLockCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, float64(7), parsed.Deadline)
	require.Equal(t, pxDuration, parsed.PX)
}

func TestProtocol_Unlock(t *testing.T) {
	unlockCmd := NewUnlock("my-dmap", "my-key", "token")

	cmd := stringToCommand(unlockCmd.Command(context.Background()).String())
	parsed, err := ParseUnlockCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, "token", parsed.Token)
}

func TestProtocol_LockLease(t *testing.T) {
	timeout := (7 * time.Second).Seconds()
	unlockCmd := NewLockLease("my-dmap", "my-key", "token", timeout)

	cmd := stringToCommand(unlockCmd.Command(context.Background()).String())
	parsed, err := ParseLockLeaseCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, "token", parsed.Token)
	require.Equal(t, timeout, parsed.Timeout)
}

func TestProtocol_PLockLease(t *testing.T) {
	timeout := (250 * time.Millisecond).Milliseconds()
	plockleaseCmd := NewPLockLease("my-dmap", "my-key", "token", timeout)

	cmd := stringToCommand(plockleaseCmd.Command(context.Background()).String())
	parsed, err := ParsePLockLeaseCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, "my-key", parsed.Key)
	require.Equal(t, "token", parsed.Token)
	require.Equal(t, timeout, parsed.Timeout)
}

func TestProtocol_Scan(t *testing.T) {
	scanCmd := NewScan(17, "my-dmap", 234)

	cmd := stringToCommand(scanCmd.Command(context.Background()).String())
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(17), parsed.PartID)
	require.Equal(t, uint64(234), parsed.Cursor)
	require.False(t, parsed.Replica)
	require.Equal(t, DefaultScanCount, parsed.Count)
	require.Equal(t, "", parsed.Match)
}

func TestProtocol_Scan_Count_Match_Replica(t *testing.T) {
	scanCmd := NewScan(17, "my-dmap", 234)
	scanCmd.SetCount(123)
	scanCmd.SetMatch("^even:")
	scanCmd.SetReplica()

	cmd := stringToCommand(scanCmd.Command(context.Background()).String())
	parsed, err := ParseScanCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-dmap", parsed.DMap)
	require.Equal(t, uint64(17), parsed.PartID)
	require.Equal(t, uint64(234), parsed.Cursor)
	require.True(t, parsed.Replica)
	require.Equal(t, 123, parsed.Count)
	require.Equal(t, "^even:", parsed.Match)
}
