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

package pubsub

import (
	"context"
	"sync"

	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/flog"
)

var (
	// PublishedTotal is the total number of published messages during the life of this instance.
	PublishedTotal = stats.NewInt64Counter()

	// CurrentSubscribers is the current number of listeners of Pub/Sub.
	CurrentSubscribers = stats.NewInt64Gauge()

	// SubscribersTotal is the total number of registered listeners during the life of this instance.
	SubscribersTotal = stats.NewInt64Counter()

	CurrentPSubscribers = stats.NewInt64Gauge()
	PSubscribersTotal   = stats.NewInt64Counter()
)

type Service struct {
	sync.RWMutex

	log    *flog.Logger
	pubsub *PubSub
	rt     *routingtable.RoutingTable
	server *server.Server
	client *server.Client
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *Service) RegisterHandlers() {
	s.server.ServeMux().HandleFunc(protocol.PubSub.Subscribe, s.subscribeCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PubSub.PSubscribe, s.psubscribeCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PubSub.Publish, s.publishCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PubSub.PublishInternal, s.publishInternalCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PubSub.PubSubChannels, s.pubsubChannelsCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PubSub.PubSubNumpat, s.pubsubNumpatCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PubSub.PubSubNumsub, s.pubsubNumsubCommandHandler)

}

func NewService(e *environment.Environment) (service.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ps := &PubSub{
		unsubscribeCallback: func() {
			CurrentSubscribers.Decrease(1)
		},
		punsubscribeCallback: func() {
			CurrentPSubscribers.Decrease(1)
		},
	}
	s := &Service{
		log:    e.Get("logger").(*flog.Logger),
		rt:     e.Get("routingtable").(*routingtable.RoutingTable),
		server: e.Get("server").(*server.Server),
		client: e.Get("client").(*server.Client),
		pubsub: ps,
		ctx:    ctx,
		cancel: cancel,
	}
	s.RegisterHandlers()
	return s, nil
}

func (s *Service) Start() error {
	// dummy implementation
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.cancel()
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}
	return nil
}

var _ service.Service = (*Service)(nil)
