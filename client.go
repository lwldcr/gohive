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
	sasl "github.com/emersion/go-sasl"
	"git.apache.org/thrift.git/lib/go/thrift"
	"net"
	"strconv"
	"errors"
	"encoding/binary"
	"fmt"
	"tcliservice"
	"bytes"
)

// SASL status definition
const (
	START = iota + 1
	OK
	BAD
	ERROR
	COMPLETE
)

// TSaslClientTransport struct
type TSaslClientTransport struct {
	*thrift.TSocket
	SaslClientFactory sasl.Client
	SaslClient sasl.Client
	Client *tcliservice.TCLIServiceClient
	Session *tcliservice.TSessionHandle
	opened bool
	User string
	Passwd string
	WriteBuffer *bytes.Buffer
	ReadBuffer *bytes.Buffer
}

// NewTSaslTransport news a pointer to TSaslClientTransport struct with given configurations
func NewTSaslTransport(host string, port int, user string, passwd string) (*TSaslClientTransport, error) {
	saslClient := sasl.NewPlainClient("", user, passwd)
	socket, err := thrift.NewTSocket(net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	trans := TSaslClientTransport{
		TSocket:socket,
		SaslClientFactory:saslClient,
		User:user,
		Passwd:passwd,
		ReadBuffer:new(bytes.Buffer),
		WriteBuffer:new(bytes.Buffer),
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
func (t *TSaslClientTransport) Flush() error {
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

	if ! t.TSocket.IsOpen() {
		if err := t.TSocket.Open(); err != nil {
			return err
		}
	}

	if t.SaslClient != nil {
		return errors.New("Already open!")
	}

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

	//fmt.Println("connection built, opening session...")

	req := tcliservice.NewTOpenSessionReq()
	req.Username = &t.User
	req.Password = &t.Passwd
	req.ClientProtocol = tcliservice.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V1

	session, err := cli.OpenSession(req)
	if err != nil {
		return err
	}

	t.Client = cli
	t.Session = session.SessionHandle
	t.opened = true
	//fmt.Println("session opened")
	return nil
}

// Close trys to close session and socket
func (t *TSaslClientTransport) Close() error {
	// close session
	req := tcliservice.NewTCloseSessionReq()
	req.SessionHandle = t.Session
	_, err := t.Client.CloseSession(req)
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
func (t *TSaslClientTransport) sendMessage(status int, body []byte) (int, error){
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
	if err := t.TSocket.Flush(); err != nil {
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
			fmt.Println(err)
			return -1, nil, err
		}
	}
	return status, payload, nil
}