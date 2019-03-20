/*
Package gohive trys to provide a HIVE client with SASL authentication via thrift.
If you have a tcliservice package generated from thrift package, it probably cannot work
without the option "hive.server2.authentication" set to NOSASL which may cause other hive-based apps
die. To avoid such situation, I found a python packge which can handle Hive authentication with SASL supported.
So this package just translates implementation of that python package "pyhs2"
(https://pypi.python.org/pypi/pyhs2) but omit some features.
It now supports PLAIN mode only, other modes MAY be included in later updates.
*/
package gohive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"

	"context"
	"github.com/apache/thrift/lib/go/thrift"
	sasl "github.com/emersion/go-sasl"
	"github.com/lwldcr/gohive/tcliservice"
)

// SASL status definition
const (
	START = iota + 1
	OK
	BAD
	ERROR
	COMPLETE

	DefaultUser = "_hive"
	DefaultPass = "_hive"
)

// TSaslClientTransport struct
type TSaslClientTransport struct {
	*thrift.TSocket
	SaslClientFactory sasl.Client
	SaslClient        sasl.Client
	Client            *tcliservice.TCLIServiceClient
	Session           *tcliservice.TSessionHandle
	opened            bool
	User              string
	Passwd            string
	WriteBuffer       *bytes.Buffer
	ReadBuffer        *bytes.Buffer
	options           Options
	Ctx               context.Context
	NeedAuth          bool
}

// NewTSaslTransport news a pointer to TSaslClientTransport struct with given configurations
func NewTSaslTransport(host string, port int, user string, passwd string, options Options) (*TSaslClientTransport, error) {
	var saslClient sasl.Client
	var needAuth bool
	if user == "" && passwd == "" {
		user = DefaultPass
		passwd = DefaultPass
	}

	if user != "" {
		saslClient = sasl.NewPlainClient("", user, passwd)
		needAuth = true
	}
	socket, err := thrift.NewTSocket(net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	trans := TSaslClientTransport{
		TSocket:           socket,
		SaslClientFactory: saslClient,
		User:              user,
		Passwd:            passwd,
		ReadBuffer:        new(bytes.Buffer),
		WriteBuffer:       new(bytes.Buffer),
		options:           options,
		Ctx:               context.Background(),
		NeedAuth:          needAuth,
	}

	return &trans, nil
}

// Write writes data into local buffer, which will be processed on Flush() called
func (t *TSaslClientTransport) Write(data []byte) (int, error) {
	return t.WriteBuffer.Write(data)
}

// Flush will flush local buffered data into transport, with a leading 4 bytes representing data length
// This is VERY import for authentication.
// If we dont send data length, the server doesnot know how long to read.
func (t *TSaslClientTransport) Flush(ctx context.Context) error {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(t.WriteBuffer.Len()))

	frame := make([]byte, 0)
	frame = append(frame, length...)
	frame = append(frame, t.WriteBuffer.Bytes()...)
	_, err := t.TSocket.Write(frame)
	if err == nil {
		t.WriteBuffer = new(bytes.Buffer)
		return nil
	}
	return err
}

// Read reads data from local buffer, will read a new data frame if needed.
func (t *TSaslClientTransport) Read(buf []byte) (int, error) {
	n, err := t.ReadBuffer.Read(buf)
	if n > 0 {
		return n, err
	}
	t.ReadFrame()
	return t.ReadBuffer.Read(buf)
}

// ReadFrame reads a frame of data into local buffer, which means first read data's length, then reads actual data.
func (t *TSaslClientTransport) ReadFrame() error {
	header := make([]byte, 4)
	_, err := t.TSocket.Read(header)
	if err != nil {
		return err
	}
	length := int(binary.BigEndian.Uint32(header))
	data := make([]byte, length)
	_, err = t.TSocket.Read(data)
	if err != nil {
		return err
	}
	_, err = t.ReadBuffer.Write(data)
	return err
}

// Open does:
// 		* open socket
//      * start SASL client
//      * send initial message
//      * open session
func (t *TSaslClientTransport) Open() error {
	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	cli := tcliservice.NewTCLIServiceClientFactory(t, protocol)

	if !t.TSocket.IsOpen() {
		if err := t.TSocket.Open(); err != nil {
			return err
		}
	}

	if t.SaslClient != nil {
		return errors.New("already open")
	}

	req := tcliservice.NewTOpenSessionReq()
	if t.NeedAuth && t.SaslClientFactory != nil {
		t.SaslClient = t.SaslClientFactory
		mech, ir, err := t.SaslClient.Start()
		if err != nil {
			return err
		}

		// send initial message
		if _, err := t.sendMessage(START, []byte(mech)); err != nil {
			return err
		}

		if _, err := t.sendMessage(OK, ir); err != nil {
			return err
		}

		for {
			status, payload, err := t.recvMessage()
			if err != nil {
				return err
			}
			if status != OK && status != COMPLETE {
				return errors.New(fmt.Sprintf("Bad status:%d %s", status, string(payload)))
			}

			if status == COMPLETE {
				break
			}

			response, err := t.SaslClient.Next(payload)
			if err != nil {
				return err
			}
			t.sendMessage(OK, response)
		}

		req.Username = &t.User
		req.Password = &t.Passwd
	}
	req.ClientProtocol = tcliservice.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V9

	session, err := cli.OpenSession(context.Background(), req)
	if err != nil {
		return err
	}

	t.Client = cli
	t.Session = session.SessionHandle
	t.opened = true
	return nil
}

// Close trys to close session and socket
func (t *TSaslClientTransport) Close() error {
	// close session
	req := tcliservice.NewTCloseSessionReq()
	req.SessionHandle = t.Session
	_, err := t.Client.CloseSession(t.Ctx, req)
	if err != nil {
		return err
	}
	t.Client = nil
	t.Session = nil

	if err := t.TSocket.Close(); err != nil {
		return err
	}
	t.opened = false
	return nil
}

// IsOpen returns if client is opened
func (t *TSaslClientTransport) IsOpen() bool {
	return t.opened
}

// sendMessage sends data length, status code and message body
func (t *TSaslClientTransport) sendMessage(status int, body []byte) (int, error) {
	data := make([]byte, 0)
	header1 := byte(status)
	header2 := make([]byte, 4)
	binary.BigEndian.PutUint32(header2, uint32(len(body)))

	data = append(data, header1)
	data = append(data, header2...)
	data = append(data, body...)

	n, err := t.TSocket.Write(data)
	if err != nil {
		return n, err
	}
	if err := t.TSocket.Flush(t.Ctx); err != nil {
		return n, err
	}
	return n, nil
}

// recvMessage receives init response from server
func (t *TSaslClientTransport) recvMessage() (int, []byte, error) {
	header := make([]byte, 5)
	_, err := t.TSocket.Read(header)
	if err != nil {
		return -1, nil, err
	}

	status := int(header[0])
	length := int(binary.BigEndian.Uint32(header[1:]))
	var payload []byte
	if length > 0 {
		payload = make([]byte, length)
		_, err := t.TSocket.Read(payload)
		if err != nil {
			return -1, nil, err
		}
	}
	return status, payload, nil
}

// Issue a query on an open connection, returning a RowSet, which
// can be later used to query the operation's status.
func (t *TSaslClientTransport) Query(query string) (RowSet, error) {
	executeReq := tcliservice.NewTExecuteStatementReq()
	executeReq.SessionHandle = t.Session
	executeReq.Statement = query

	resp, err := t.Client.ExecuteStatement(t.Ctx, executeReq)
	if err != nil {
		return nil, fmt.Errorf("error in ExecuteStatement: %+v, %v", resp, err)
	}

	if !isSuccessStatus(*resp.Status) {
		return nil, fmt.Errorf("error from server: %s", resp.Status.String())
	}

	return newRowSet(t.Client, resp.OperationHandle, t.options), nil
}

func isSuccessStatus(p tcliservice.TStatus) bool {
	status := p.GetStatusCode()
	return status == tcliservice.TStatusCode_SUCCESS_STATUS || status == tcliservice.TStatusCode_SUCCESS_WITH_INFO_STATUS
}
