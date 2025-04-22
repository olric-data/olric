package protocol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocol_ParsePublishCommand(t *testing.T) {
	publishCmd := NewPublish("my-pubsub", "my-message")

	cmd := stringToCommand(publishCmd.Command(context.Background()).String())
	parsed, err := ParsePublishCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-pubsub", parsed.Channel)
	require.Equal(t, "my-message", parsed.Message)
}

func TestProtocol_ParsePublishInternalCommand(t *testing.T) {
	publishIntCmd := NewPublishInternal("my-pubsub", "my-message")

	cmd := stringToCommand(publishIntCmd.Command(context.Background()).String())
	parsed, err := ParsePublishInternalCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "my-pubsub", parsed.Channel)
	require.Equal(t, "my-message", parsed.Message)
}

func TestProtocol_ParseSubscribeCommand(t *testing.T) {
	subscribeCmd := NewSubscribe("channel-1", "channel-2", "channel-3")

	cmd := stringToCommand(subscribeCmd.Command(context.Background()).String())
	parsed, err := ParseSubscribeCommand(cmd)
	require.NoError(t, err)

	channels := []string{"channel-1", "channel-2", "channel-3"}
	require.Equal(t, channels, parsed.Channels)
}

func TestProtocol_ParsePSubscribeCommand(t *testing.T) {
	psubscribeCmd := NewPSubscribe("ch?nnel-*")

	cmd := stringToCommand(psubscribeCmd.Command(context.Background()).String())
	parsed, err := ParsePSubscribeCommand(cmd)
	require.NoError(t, err)

	patterns := []string{"ch?nnel-*"}
	require.Equal(t, patterns, parsed.Patterns)
}

func TestProtocol_PubSubChannels(t *testing.T) {
	pubsubChannelsCmd := NewPubSubChannels()

	cmd := stringToCommand(pubsubChannelsCmd.Command(context.Background()).String())
	parsed, err := ParsePubSubChannelsCommand(cmd)
	require.NoError(t, err)
	require.Empty(t, parsed.Pattern)
}

func TestProtocol_PubSubChannels_Patterns(t *testing.T) {
	pubsubChannelsCmd := NewPubSubChannels()
	pubsubChannelsCmd.SetPattern("ch?nnel-*")

	cmd := stringToCommand(pubsubChannelsCmd.Command(context.Background()).String())
	parsed, err := ParsePubSubChannelsCommand(cmd)
	require.NoError(t, err)

	require.Equal(t, "ch?nnel-*", parsed.Pattern)
}

func TestProtocol_PubSubNumpat(t *testing.T) {
	pubsubNumpatCmd := NewPubSubNumpat()

	cmd := stringToCommand(pubsubNumpatCmd.Command(context.Background()).String())
	_, err := ParsePubSubNumpatCommand(cmd)
	require.NoError(t, err)
}

func TestProtocol_PubSubNumsub(t *testing.T) {
	pubsubNumsubCmd := NewPubSubNumsub("channel-1", "channel-2", "channel-3")

	cmd := stringToCommand(pubsubNumsubCmd.Command(context.Background()).String())
	parsed, err := ParsePubSubNumsubCommand(cmd)
	require.NoError(t, err)

	channels := []string{"channel-1", "channel-2", "channel-3"}
	require.Equal(t, channels, parsed.Channels)
}
