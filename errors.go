package producer

import (
	"fmt"
)

type ErrCode string

const ErrShardNotFound ErrCode = "OcShardMappingError"

type Error struct {
	err       error
	ErrorCode ErrCode
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %v", e.ErrorCode, e.err)
}
