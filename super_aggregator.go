package producer

import "fmt"

type SuperAggregator struct {
	aggregators []*Aggregator
}

func newSuperAggregator(shards []*Shard) *SuperAggregator {
	aggregators := []*Aggregator{}
	for _, shard := range shards {
		aggregators = append(aggregators, &Aggregator{Shard: shard})
	}
	sa := &SuperAggregator{aggregators}
	return sa
}

func (sa *SuperAggregator) Size() int {
	size := 0
	for _, a := range sa.aggregators {
		size += a.Size()
	}
	return size
}

func (sa *SuperAggregator) Count() int {
	length := 0
	for _, a := range sa.aggregators {
		length += a.Count()
	}
	return length
}

func (sa *SuperAggregator) Drain() ([]*AggregatedBatch, error) {
	batches := []*AggregatedBatch{}
	for _, a := range sa.aggregators {
		batch, err := a.Drain()
		if err != nil {
			return batches, err
		}
		if batch != nil {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func (sa *SuperAggregator) DrainIfConstrained(nbytes int, maxBatchSize int) ([]*AggregatedBatch, error) {
	batches := []*AggregatedBatch{}
	for _, a := range sa.aggregators {
		batch, err := a.DrainIfConstrained(nbytes, maxBatchSize)
		if err != nil {
			return batches, err
		}
		if batch != nil {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func (sa *SuperAggregator) Put(data []byte, partitionKey string) *Error {
	aggregator, err := sa.getAggregatorForPartitionKey(partitionKey)
	if err != nil {
		return &Error{err, ErrShardNotFound}
	}
	aggregator.Put(data, partitionKey)
	return nil
}

func (sa *SuperAggregator) getAggregatorForPartitionKey(partitionKey string) (*Aggregator, error) {
	for _, aggregator := range sa.aggregators {
		ok, err := aggregator.Shard.belongsToShard(partitionKey)
		if err != nil {
			return nil, err
		}
		if ok {
			return aggregator, nil
		}
	}
	return nil, fmt.Errorf("no aggregator/shard found for parition key %s", partitionKey)
}
