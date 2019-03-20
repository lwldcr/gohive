package gohive

/*
rowset.go comes from github.com/derekgr/hivething, tiny changes
*/
import (
	"errors"
	"fmt"
	"log"
	"time"

	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/lwldcr/gohive/tcliservice"
)

// Options for opened Hive sessions.
type Options struct {
	PollIntervalSeconds int64
	BatchSize           int64
}

var (
	DefaultOptions = Options{PollIntervalSeconds: 5, BatchSize: 10000}
)

type rowSet struct {
	thrift    *tcliservice.TCLIServiceClient
	operation *tcliservice.TOperationHandle
	options   Options

	columns      []*tcliservice.TColumnDesc
	columnStrs   []string
	columnValues [][]interface{}

	numRows int
	offset  int
	rowSet  *tcliservice.TRowSet
	hasMore bool
	ready   bool

	nextRow []interface{}

	Ctx context.Context
}

// A RowSet represents an asyncronous hive operation. You can
// Reattach to a previously submitted hive operation if you
// have a valid thrift client, and the serialized Handle()
// from the prior operation.
type RowSet interface {
	Handle() ([]byte, error)
	Columns() []string
	Next() bool
	Scan(dest ...interface{}) error
	Poll() (*Status, error)
	Wait() (*Status, error)
	Close() error
}

// Represents job status, including success state and time the
// status was updated.
type Status struct {
	state *tcliservice.TOperationState
	Error error
	At    time.Time
}

func newRowSet(thrift *tcliservice.TCLIServiceClient, operation *tcliservice.TOperationHandle, options Options) RowSet {
	return &rowSet{thrift: thrift, operation: operation, options: options, offset: 0, hasMore: true, ready: false,
		Ctx: context.Background()}
}

// Construct a RowSet for a previously submitted operation, using the prior operation's Handle()
// and a valid thrift client to a hive service that is aware of the operation.
func Reattach(conn *TSaslClientTransport, handle []byte) (RowSet, error) {
	operation, err := deserializeOp(handle)
	if err != nil {
		return nil, err
	}

	return newRowSet(conn.Client, operation, conn.options), nil
}

// Issue a thrift call to check for the job's current status.
func (r *rowSet) Poll() (*Status, error) {
	req := tcliservice.NewTGetOperationStatusReq()
	req.OperationHandle = r.operation

	resp, err := r.thrift.GetOperationStatus(r.Ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error getting status: %+v, %v", resp, err)
	}

	if !isSuccessStatus(*resp.Status) {
		return nil, fmt.Errorf("GetStatus call failed: %s", resp.Status.String())
	}

	if resp.OperationState == nil {
		return nil, errors.New("no error from GetStatus, but nil status!")
	}

	return &Status{resp.OperationState, nil, time.Now()}, nil
}

// Wait until the job is complete, one way or another, returning Status and error.
func (r *rowSet) Wait() (*Status, error) {
	for {
		status, err := r.Poll()

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				// Fetch operation metadata.
				metadataReq := tcliservice.NewTGetResultSetMetadataReq()
				metadataReq.OperationHandle = r.operation

				metadataResp, err := r.thrift.GetResultSetMetadata(r.Ctx, metadataReq)
				if err != nil {
					return nil, err
				}

				if !isSuccessStatus(*metadataResp.Status) {
					return nil, fmt.Errorf("GetResultSetMetadata failed: %s", metadataResp.Status.String())
				}

				r.columns = metadataResp.Schema.Columns
				r.ready = true

				return status, nil
			}
			return nil, fmt.Errorf("query failed execution: %s", status.state.String())
		}

		time.Sleep(time.Duration(r.options.PollIntervalSeconds) * time.Second)
	}
}

func (r *rowSet) waitForSuccess() error {
	if !r.ready {
		status, err := r.Wait()
		if err != nil {
			return err
		}
		if !status.IsSuccess() || !r.ready {
			return fmt.Errorf("unsuccessful query execution: %+v", status)
		}
	}

	return nil
}

// Prepares a row for scanning into memory, by reading data from hive if
// the operation is successful, blocking until the operation is
// complete, if necessary.
// Returns true is a row is available to Scan(), and false if the
// results are empty or any other error occurs.
func (r *rowSet) Next() bool {
	if err := r.waitForSuccess(); err != nil {
		return false
	}

	//fmt.Println("RRRR", r.rowSet == nil, r.offset, r.numRows)
	if r.rowSet == nil || r.offset >= r.numRows {
		if !r.hasMore {
			return false
		}

		fetchReq := tcliservice.NewTFetchResultsReq()
		fetchReq.OperationHandle = r.operation
		fetchReq.Orientation = tcliservice.TFetchOrientation_FETCH_NEXT
		fetchReq.MaxRows = r.options.BatchSize

		resp, err := r.thrift.FetchResults(r.Ctx, fetchReq)
		if err != nil {
			log.Printf("FetchResults failed: %v\n", err)
			return false
		}

		if !isSuccessStatus(*resp.Status) {
			log.Printf("FetchResults failed: %s\n", resp.Status.String())
			return false
		}

		r.offset = 0
		//fmt.Println("results;", resp.GetResults(), len(resp.Results.Columns)) //, len(resp.GetResults().GetColumns()[0].StringVal.Values))
		r.rowSet = resp.Results
		r.hasMore = *resp.HasMoreRows
	}

	//fmt.Println("offset of rows:", r.offset)
	//fmt.Println("length:", len(r.rowSet.Rows))
	//row := r.rowSet.Rows[r.offset]
	//column := r.rowSet.Columns[r.offset]
	r.nextRow = make([]interface{}, len(r.Columns()))
	if r.columnValues == nil {
		valueSets, err := processColumnValues(r.rowSet.Columns)
		if err != nil || len(valueSets) <= 0 {
			return false
		}
		r.columnValues = valueSets
		r.numRows = len(r.columnValues[0])
	}

	row := make([]interface{}, 0, len(r.columnValues))
	for _, col := range r.columnValues {
		row = append(row, col[r.offset])
	}
	r.nextRow = row
	r.offset++

	return true
}

// Scan the last row prepared via Next() into the destination(s) provided,
// which must be pointers to value types, as in database.sql. Further,
// only pointers of the following types are supported:
// 	- int, int16, int32, int64
// 	- string, []byte
// 	- float64
//	 - bool
func (r *rowSet) Scan(dest ...interface{}) error {
	// TODO: Add type checking and conversion between compatible
	// types where possible, as well as some common error checking,
	// like passing nil. database/sql's method is very convenient,
	// for example: http://golang.org/src/pkg/database/sql/convert.go, like 85
	if r.nextRow == nil {
		return errors.New("no row to scan! Did you call Next() first?")
	}

	if len(dest) != len(r.nextRow) {
		return fmt.Errorf("can't scan into %d arguments with input of length %d", len(dest), len(r.nextRow))
	}

	for i, val := range r.nextRow {
		d := dest[i]
		switch dt := d.(type) {
		case *string:
			switch st := val.(type) {
			case string:
				*dt = st
			default:
				*dt = fmt.Sprintf("%v", val)
			}
		case *[]byte:
			*dt = []byte(val.(string))
		case *int:
			*dt = int(val.(int32))
		case *int64:
			*dt = val.(int64)
		case *int32:
			*dt = val.(int32)
		case *int16:
			*dt = val.(int16)
		case *float64:
			*dt = val.(float64)
		case *bool:
			*dt = val.(bool)
		default:
			return fmt.Errorf("Can't scan value of type %T with value %v", dt, val)
		}
	}

	return nil
}

// Returns the names of the columns for the given operation,
// blocking if necessary until the information is available.
func (r *rowSet) Columns() []string {
	if r.columnStrs == nil {
		if err := r.waitForSuccess(); err != nil {
			return nil
		}

		ret := make([]string, len(r.columns))
		for i, col := range r.columns {
			ret[i] = col.ColumnName
		}

		r.columnStrs = ret
	}

	return r.columnStrs
}

// Return a serialized representation of an identifier that can later
// be used to reattach to a running operation. This identifier and
// serialized representation should be considered opaque by users.
func (r *rowSet) Handle() ([]byte, error) {
	return serializeOp(r.Ctx, r.operation)
}

// processColumnValues processes results columns and converts into slices of values
func processColumnValues(cols []*tcliservice.TColumn) ([][]interface{}, error) {
	valueSets := make([][]interface{}, 0, len(cols))
	for _, col := range cols {
		values, err := convertColumnValues(col)
		if err != nil {
			return valueSets, err
		}
		if len(values) > 0 {
			valueSets = append(valueSets, values)
		}
	}
	return valueSets, nil
}

// convertColumnValues converts given column values into interface{} slice
func convertColumnValues(col *tcliservice.TColumn) ([]interface{}, error) {
	var size int
	var dest []interface{}
	switch {
	case col.StringVal != nil && len(col.StringVal.Values) > 0:
		size = len(col.StringVal.Values)
		dest = make([]interface{}, size)
		for i, v := range col.StringVal.Values {
			dest[i] = v
		}
	case col.BinaryVal != nil && len(col.BinaryVal.Values) > 0:
		size = len(col.BinaryVal.Values)
		dest = make([]interface{}, size)
		for i, v := range col.BinaryVal.Values {
			dest[i] = v
		}
	case col.BoolVal != nil && len(col.BoolVal.Values) > 0:
		size = len(col.BoolVal.Values)
		dest = make([]interface{}, size)
		for i, v := range col.BoolVal.Values {
			dest[i] = v
		}
	case col.ByteVal != nil && len(col.ByteVal.Values) > 0:
		size = len(col.ByteVal.Values)
		dest = make([]interface{}, size)
		for i, v := range col.ByteVal.Values {
			dest[i] = v
		}
	case col.DoubleVal != nil && len(col.DoubleVal.Values) > 0:
		size = len(col.DoubleVal.Values)
		dest = make([]interface{}, size)
		for i, v := range col.DoubleVal.Values {
			dest[i] = v
		}
	case col.I16Val != nil && len(col.I16Val.Values) > 0:
		size = len(col.I16Val.Values)
		dest = make([]interface{}, size)
		for i, v := range col.I16Val.Values {
			dest[i] = v
		}
	case col.I32Val != nil && len(col.I32Val.Values) > 0:
		size = len(col.I32Val.Values)
		dest = make([]interface{}, size)
		for i, v := range col.I32Val.Values {
			dest[i] = v
		}
	case col.I64Val != nil && len(col.I64Val.Values) > 0:
		size = len(col.I64Val.Values)
		dest = make([]interface{}, size)
		for i, v := range col.I64Val.Values {
			dest[i] = v
		}
	}
	return dest, nil
}

// Returns a string representation of operation status.
func (s Status) String() string {
	if s.state == nil {
		return "unknown"
	}
	return s.state.String()
}

// Returns true if the job has completed or failed.
func (s Status) IsComplete() bool {
	if s.state == nil {
		return false
	}

	switch *s.state {
	case tcliservice.TOperationState_FINISHED_STATE,
		tcliservice.TOperationState_CANCELED_STATE,
		tcliservice.TOperationState_CLOSED_STATE,
		tcliservice.TOperationState_ERROR_STATE:
		return true
	}

	return false
}

// Returns true if the job compelted successfully.
func (s Status) IsSuccess() bool {
	if s.state == nil {
		return false
	}

	return *s.state == tcliservice.TOperationState_FINISHED_STATE
}

func deserializeOp(handle []byte) (*tcliservice.TOperationHandle, error) {
	ser := thrift.NewTDeserializer()
	var val tcliservice.TOperationHandle

	if err := ser.Read(&val, handle); err != nil {
		return nil, err
	}

	return &val, nil
}

func serializeOp(ctx context.Context, operation *tcliservice.TOperationHandle) ([]byte, error) {
	ser := thrift.NewTSerializer()
	return ser.Write(ctx, operation)
}

// Close do close operation
func (r *rowSet) Close() error {
	closeOperationReq := tcliservice.NewTCloseOperationReq()
	closeOperationReq.OperationHandle = r.operation

	_, err := r.thrift.CloseOperation(r.Ctx, closeOperationReq)

	return err
}
