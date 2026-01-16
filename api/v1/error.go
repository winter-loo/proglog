package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

// implement a custom error so a user could see extra information
type ErrLsnOutOfRange struct {
	Lsn uint64
}

func (e ErrLsnOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("lsn out of range: %d", e.Lsn),
	)
	msg := fmt.Sprintf(
		"The requested lsn is outside the log's range: %d",
		e.Lsn,
	)
	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return std
}

func (e ErrLsnOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
