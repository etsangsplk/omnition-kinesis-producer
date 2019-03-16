package producer

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func getShardsForStream(config *Config) ([]*Shard, error) {
	k, ok := config.Client.(*kinesis.Kinesis)
	if !ok {
		return nil, fmt.Errorf("config.Putter must be a pointer to kinesis.Kinesis")
	}

	out, err := k.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(config.StreamName),
	})
	if err != nil {
		return nil, err
	}

	// TODO: Handle pagination and fetch all shards
	shards := []*Shard{}
	for _, s := range out.StreamDescription.Shards {
		if s.SequenceNumberRange.EndingSequenceNumber != nil {
			continue
		}

		shards = append(shards, &Shard{
			shardId:         *s.ShardId,
			startingHashKey: toBigInt(*s.HashKeyRange.StartingHashKey),
			endingHashKey:   toBigInt(*s.HashKeyRange.EndingHashKey),
		})
	}

	return shards, nil
}

func toBigInt(key string) *big.Int {
	num := big.NewInt(0)
	num.SetString(key, 10)
	return num
}

type Shard struct {
	shardId         string
	startingHashKey *big.Int
	endingHashKey   *big.Int
}

func (s *Shard) belongsToShard(partitionKey string) (bool, error) {
	key, err := s.partitionKeyToHashKey(partitionKey)
	if err != nil {
		return false, err
	}
	return key.Cmp(s.startingHashKey) >= 0 && key.Cmp(s.endingHashKey) <= 0, nil
}

func (s *Shard) partitionKeyToHashKey(partitionKey string) (*big.Int, error) {
	bi := big.NewInt(0)
	h := md5.New()
	_, err := io.WriteString(h, partitionKey)
	if err != nil {
		return nil, err
	}
	hexstr := hex.EncodeToString(h.Sum(nil))
	bi.SetString(hexstr, 16)
	return bi, nil
}
