package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"

	witnessconfig "github.com/openfga/openfga/witness/config"
	"github.com/openfga/openfga/witness/ocsf"
)

type FirehoseClient interface {
	PutRecordBatch(ctx context.Context, records []json.RawMessage) error
}

type awsFirehoseClient struct {
	client     *firehose.Client
	streamName string
}

func (c *awsFirehoseClient) PutRecordBatch(ctx context.Context, records []json.RawMessage) error {
	fhRecords := make([]types.Record, len(records))
	for i, r := range records {
		fhRecords[i] = types.Record{Data: append(r, '\n')}
	}
	_, err := c.client.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(c.streamName),
		Records:            fhRecords,
	})
	return err
}

type FirehoseSink struct {
	client        FirehoseClient
	batchSize     int
	flushInterval time.Duration

	mu       sync.Mutex
	buffer   []json.RawMessage
	done     chan struct{}
	closed   atomic.Bool
	closeOnce sync.Once
	loopDone chan struct{} // signals that flushLoop has exited
}

func NewFirehoseSink(cfg witnessconfig.FirehoseConfig) (*FirehoseSink, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	client := &awsFirehoseClient{
		client:     firehose.NewFromConfig(awsCfg),
		streamName: cfg.StreamName,
	}
	return NewFirehoseSinkWithClient(client, cfg.BatchSize, cfg.FlushInterval), nil
}

func NewFirehoseSinkWithClient(client FirehoseClient, batchSize int, flushInterval time.Duration) *FirehoseSink {
	s := &FirehoseSink{
		client:        client,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		done:          make(chan struct{}),
		loopDone:      make(chan struct{}),
	}
	go s.flushLoop()
	return s
}

func (s *FirehoseSink) Emit(_ context.Context, event ocsf.APIActivityEvent) error {
	if s.closed.Load() {
		return fmt.Errorf("sink is closed")
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.buffer = append(s.buffer, data)
	shouldFlush := len(s.buffer) >= s.batchSize
	s.mu.Unlock()

	if shouldFlush {
		go s.flush(context.Background())
	}
	return nil
}

func (s *FirehoseSink) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.done)

		// Wait for the flushLoop goroutine to exit before the final flush,
		// so there are no concurrent flushes.
		<-s.loopDone

		err = s.flush(ctx)
	})
	return err
}

func (s *FirehoseSink) flush(ctx context.Context) error {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return nil
	}
	batch := s.buffer
	s.buffer = nil
	s.mu.Unlock()

	if err := s.client.PutRecordBatch(ctx, batch); err != nil {
		log.Printf("openfga-witness: firehose PutRecordBatch failed (%d records): %v", len(batch), err)
		return err
	}
	return nil
}

func (s *FirehoseSink) flushLoop() {
	defer close(s.loopDone)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.flush(context.Background())
		case <-s.done:
			return
		}
	}
}
