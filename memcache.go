// A memcached binary protocol client.
package memcache

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

type CommandCode uint8

const (
	GET        = CommandCode(0x00)
	SET        = CommandCode(0x01)
	ADD        = CommandCode(0x02)
	REPLACE    = CommandCode(0x03)
	DELETE     = CommandCode(0x04)
	INCREMENT  = CommandCode(0x05)
	DECREMENT  = CommandCode(0x06)
	QUIT       = CommandCode(0x07)
	FLUSH      = CommandCode(0x08)
	GETQ       = CommandCode(0x09)
	NOOP       = CommandCode(0x0a)
	VERSION    = CommandCode(0x0b)
	GETK       = CommandCode(0x0c)
	GETKQ      = CommandCode(0x0d)
	APPEND     = CommandCode(0x0e)
	PREPEND    = CommandCode(0x0f)
	STAT       = CommandCode(0x10)
	SETQ       = CommandCode(0x11)
	ADDQ       = CommandCode(0x12)
	REPLACEQ   = CommandCode(0x13)
	DELETEQ    = CommandCode(0x14)
	INCREMENTQ = CommandCode(0x15)
	DECREMENTQ = CommandCode(0x16)
	QUITQ      = CommandCode(0x17)
	FLUSHQ     = CommandCode(0x18)
	APPENDQ    = CommandCode(0x19)
	PREPENDQ   = CommandCode(0x1a)
	RGET       = CommandCode(0x30)
	RSET       = CommandCode(0x31)
	RSETQ      = CommandCode(0x32)
	RAPPEND    = CommandCode(0x33)
	RAPPENDQ   = CommandCode(0x34)
	RPREPEND   = CommandCode(0x35)
	RPREPENDQ  = CommandCode(0x36)
	RDELETE    = CommandCode(0x37)
	RDELETEQ   = CommandCode(0x38)
	RINCR      = CommandCode(0x39)
	RINCRQ     = CommandCode(0x3a)
	RDECR      = CommandCode(0x3b)
	RDECRQ     = CommandCode(0x3c)
    GETL       = CommandCode(0x94)

	TAP_CONNECT          = CommandCode(0x40)
	TAP_MUTATION         = CommandCode(0x41)
	TAP_DELETE           = CommandCode(0x42)
	TAP_FLUSH            = CommandCode(0x43)
	TAP_OPAQUE           = CommandCode(0x44)
	TAP_VBUCKET_SET      = CommandCode(0x45)
	TAP_CHECKPOINT_START = CommandCode(0x46)
	TAP_CHECKPOINT_END   = CommandCode(0x47)
)

type Status uint16

const (
	SUCCESS         = Status(0x00)
	KEY_ENOENT      = Status(0x01)
	KEY_EEXISTS     = Status(0x02)
	E2BIG           = Status(0x03)
	EINVAL          = Status(0x04)
	NOT_STORED      = Status(0x05)
	DELTA_BADVAL    = Status(0x06)
	NOT_MY_VBUCKET  = Status(0x07)
	UNKNOWN_COMMAND = Status(0x81)
	ENOMEM          = Status(0x82)
	TMPFAIL         = Status(0x86)
)

const (
	BACKFILL          = 0x01
	DUMP              = 0x02
	LIST_VBUCKETS     = 0x04
	TAKEOVER_VBUCKETS = 0x08
	SUPPORT_ACK       = 0x10
	REQUEST_KEYS_ONLY = 0x20
	CHECKPOINT        = 0x40
	REGISTERED_CLIENT = 0x80
)

// Number of bytes in a binary protocol header.
const HDR_LEN = 24

// Mapping of CommandCode -> name of command (not exhaustive)
var CommandNames map[CommandCode]string

var StatusNames map[Status]string

func init() {
	CommandNames = make(map[CommandCode]string)
	CommandNames[GET] = "GET"
	CommandNames[SET] = "SET"
	CommandNames[ADD] = "ADD"
	CommandNames[REPLACE] = "REPLACE"
	CommandNames[DELETE] = "DELETE"
	CommandNames[INCREMENT] = "INCREMENT"
	CommandNames[DECREMENT] = "DECREMENT"
	CommandNames[QUIT] = "QUIT"
	CommandNames[FLUSH] = "FLUSH"
	CommandNames[GETQ] = "GETQ"
	CommandNames[NOOP] = "NOOP"
	CommandNames[VERSION] = "VERSION"
	CommandNames[GETK] = "GETK"
	CommandNames[GETKQ] = "GETKQ"
	CommandNames[APPEND] = "APPEND"
	CommandNames[PREPEND] = "PREPEND"
	CommandNames[STAT] = "STAT"
	CommandNames[SETQ] = "SETQ"
	CommandNames[ADDQ] = "ADDQ"
	CommandNames[REPLACEQ] = "REPLACEQ"
	CommandNames[DELETEQ] = "DELETEQ"
	CommandNames[INCREMENTQ] = "INCREMENTQ"
	CommandNames[DECREMENTQ] = "DECREMENTQ"
	CommandNames[QUITQ] = "QUITQ"
	CommandNames[FLUSHQ] = "FLUSHQ"
	CommandNames[APPENDQ] = "APPENDQ"
	CommandNames[PREPENDQ] = "PREPENDQ"
	CommandNames[RGET] = "RGET"
	CommandNames[RSET] = "RSET"
	CommandNames[RSETQ] = "RSETQ"
	CommandNames[RAPPEND] = "RAPPEND"
	CommandNames[RAPPENDQ] = "RAPPENDQ"
	CommandNames[RPREPEND] = "RPREPEND"
	CommandNames[RPREPENDQ] = "RPREPENDQ"
	CommandNames[RDELETE] = "RDELETE"
	CommandNames[RDELETEQ] = "RDELETEQ"
	CommandNames[RINCR] = "RINCR"
	CommandNames[RINCRQ] = "RINCRQ"
	CommandNames[RDECR] = "RDECR"
	CommandNames[RDECRQ] = "RDECRQ"
	CommandNames[GETL] = "GETL"

	CommandNames[TAP_CONNECT] = "TAP_CONNECT"
	CommandNames[TAP_MUTATION] = "TAP_MUTATION"
	CommandNames[TAP_DELETE] = "TAP_DELETE"
	CommandNames[TAP_FLUSH] = "TAP_FLUSH"
	CommandNames[TAP_OPAQUE] = "TAP_OPAQUE"
	CommandNames[TAP_VBUCKET_SET] = "TAP_VBUCKET_SET"
	CommandNames[TAP_CHECKPOINT_START] = "TAP_CHECKPOINT_START"
	CommandNames[TAP_CHECKPOINT_END] = "TAP_CHECKPOINT_END"

	StatusNames = make(map[Status]string)
	StatusNames[SUCCESS] = "SUCCESS"
	StatusNames[KEY_ENOENT] = "KEY_ENOENT"
	StatusNames[KEY_EEXISTS] = "KEY_EEXISTS"
	StatusNames[E2BIG] = "E2BIG"
	StatusNames[EINVAL] = "EINVAL"
	StatusNames[NOT_STORED] = "NOT_STORED"
	StatusNames[DELTA_BADVAL] = "DELTA_BADVAL"
	StatusNames[NOT_MY_VBUCKET] = "NOT_MY_VBUCKET"
	StatusNames[UNKNOWN_COMMAND] = "UNKNOWN_COMMAND"
	StatusNames[ENOMEM] = "ENOMEM"
	StatusNames[TMPFAIL] = "TMPFAIL"

}

// String an op code.
func (o CommandCode) String() (rv string) {
	rv = CommandNames[o]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(o))
	}
	return rv
}

// String a status code.
func (s Status) String() (rv string) {
	rv = StatusNames[s]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(s))
	}
	return rv
}

type Request struct {
	Opcode            CommandCode
	Cas               uint64
	Opaque            uint32
	VBucket           uint16
	Extras, Key, Body []byte
}

func (req *Request) Size() int {
	return HDR_LEN + len(req.Extras) + len(req.Key) + len(req.Body)
}

func (req Request) String() string {
	return fmt.Sprintf("{Request opcode=%s, bodylen=%d, key='%s'}", req.Opcode, len(req.Body), req.Key)
}

func (req *Request) fillHeaderBytes(data []byte) int {
	pos := 0
	data[pos] = REQ_MAGIC
	pos++
	data[pos] = byte(req.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], uint16(len(req.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(req.Extras))
	pos++
	// data[pos] = 0
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], req.VBucket)
	pos += 2

	// 8
	binary.BigEndian.PutUint32(data[pos:pos+4],
		uint32(len(req.Body)+len(req.Key)+len(req.Extras)))
	pos += 4

	// 12
	binary.BigEndian.PutUint32(data[pos:pos+4], req.Opaque)
	pos += 4

	// 16
	if req.Cas != 0 {
		binary.BigEndian.PutUint64(data[pos:pos+8], req.Cas)
	}
	pos += 8

	if len(req.Extras) > 0 {
		copy(data[pos:pos+len(req.Extras)], req.Extras)
		pos += len(req.Extras)
	}

	if len(req.Key) > 0 {
		copy(data[pos:pos+len(req.Key)], req.Key)
		pos += len(req.Key)
	}
	return pos
}

// The wire representation of the header (with the extras and key)
func (req *Request) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(req.Extras)+len(req.Key))

	req.fillHeaderBytes(data)

	return data
}

// The wire representation of this request.
func (req *Request) Bytes() []byte {
	data := make([]byte, req.Size())

	pos := req.fillHeaderBytes(data)

	if len(req.Body) > 0 {
		copy(data[pos:pos+len(req.Body)], req.Body)
	}

	return data
}

func getResponse(s io.Reader, buf []byte) (rv *Response, err error) {
	_, err = io.ReadFull(s, buf)
	if err != nil {
		return rv, err
	}
	rv, err = grokHeader(buf)
	if err != nil {
		return rv, err
	}
	err = readContents(s, rv)
	return rv, err
}

func readContents(s io.Reader, res *Response) error {
	if len(res.Extras) > 0 {
		_, err := io.ReadFull(s, res.Extras)
		if err != nil {
			return err
		}
	}
	if len(res.Key) > 0 {
		_, err := io.ReadFull(s, res.Key)
		if err != nil {
			return err
		}
	}
	_, err := io.ReadFull(s, res.Body)
	return err
}

func grokHeader(hdrBytes []byte) (rv *Response, err error) {
	if hdrBytes[0] != RES_MAGIC {
		return rv, fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
	}
	rv = &Response{
		Opcode: CommandCode(hdrBytes[1]),
		Key:    make([]byte, binary.BigEndian.Uint16(hdrBytes[2:4])),
		Extras: make([]byte, hdrBytes[4]),
		Status: Status(binary.BigEndian.Uint16(hdrBytes[6:8])),
		Opaque: binary.BigEndian.Uint32(hdrBytes[12:16]),
		Cas:    binary.BigEndian.Uint64(hdrBytes[16:24]),
	}
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:12]) -
		uint32(len(rv.Key)+len(rv.Extras))
	rv.Body = make([]byte, bodyLen)

	return
}

func transmitRequest(o io.Writer, req *Request) (err error) {
	if len(req.Body) < 128 {
		_, err = o.Write(req.Bytes())
	} else {
		_, err = o.Write(req.HeaderBytes())
		if err == nil && len(req.Body) > 0 {
			_, err = o.Write(req.Body)
		}
	}
	return
}

const bufsize = 1024

// The Client itself.
type Client struct {
	conn io.ReadWriteCloser

	hdrBuf []byte
}

// Connect to a memcached server.
func Connect(prot, dest string) (rv *Client, err error) {
	conn, err := net.Dial(prot, dest)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		hdrBuf: make([]byte, HDR_LEN),
	}, nil
}

// Close the connection when you're done.
func (c *Client) Close() {
	c.conn.Close()
}

// Send a custom request and get the response.
func (client *Client) Send(req *Request) (rv *Response, err error) {
	err = transmitRequest(client.conn, req)
	if err != nil {
		return
	}
	return getResponse(client.conn, client.hdrBuf)
}

// Send a request, but do not wait for a response.
func (client *Client) Transmit(req *Request) error {
	return transmitRequest(client.conn, req)
}

// Receive a response
func (client *Client) Receive() (*Response, error) {
	return getResponse(client.conn, client.hdrBuf)
}


// Tap

func (client *Client) Tap(vb []uint16, backfill bool) (<-chan *Response, error) {
    var extras, body []byte
    var ch <-chan *Response
    client.Send(&Request{
        Opcode: TAP_CONNECT,
        VBucket:0,
        Key:[]byte{},
        Cas: 0,
        Opaque: 0,
        Extras: extras,
        Body: body})
    return ch,nil
}

// Get the value for a key.
func (client *Client) Get(vb uint16, key string) (*Response, error) {
	return client.Send(&Request{
		Opcode:  GET,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{},
		Body:    []byte{}})
}

// Getl the value for a key.
func (client *Client) Getl(vb uint16, key string, expiry uint32) (*Response, error) {
    body := []byte{0,0,0,0}
    binary.BigEndian.PutUint32(body, expiry)

	return client.Send(&Request{
		Opcode:  GETL,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{},
		Body:    body})
}


// Delete a key.
func (client *Client) Del(vb uint16, key string) (*Response, error) {
	return client.Send(&Request{
		Opcode:  DELETE,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{},
		Body:    []byte{}})
}

func (client *Client) store(opcode CommandCode, vb uint16,
	key string, flags int, exp int, body []byte) (*Response, error) {

	req := &Request{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return client.Send(req)
}

// Increment a value.
func (client *Client) Incr(vb uint16, key string,
	amt, def uint64, exp int) (uint64, error) {

	req := &Request{
		Opcode:  INCREMENT,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  make([]byte, 8+8+4),
		Body:    []byte{}}
	binary.BigEndian.PutUint64(req.Extras[:8], amt)
	binary.BigEndian.PutUint64(req.Extras[8:16], def)
	binary.BigEndian.PutUint32(req.Extras[16:20], uint32(exp))

	resp, err := client.Send(req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Add a value for a key (store if not exists).
func (client *Client) Add(vb uint16, key string, flags int, exp int,
	body []byte) (*Response, error) {
	return client.store(ADD, vb, key, flags, exp, body)
}

// Set the value for a key.
func (client *Client) Set(vb uint16, key string, flags int, exp int,
	body []byte) (*Response, error) {
	return client.store(SET, vb, key, flags, exp, body)
}

// Get keys in bulk
func (client *Client) GetBulk(vb uint16, keys []string) (map[string]*Response, error) {
	terminalOpaque := uint32(len(keys) + 5)
	rv := map[string]*Response{}
	wg := sync.WaitGroup{}
	going := true

	defer func() {
		going = false
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for going {
			res, err := client.Receive()
			if err != nil {
				return
			}
			if res.Opaque == terminalOpaque {
				return
			}
			if res.Opcode != GETQ {
				log.Panicf("Unexpected opcode in GETQ response: %+v",
					res)
			}
			rv[keys[res.Opaque]] = res
		}
	}()

	for i, k := range keys {
		err := client.Transmit(&Request{
			Opcode:  GETQ,
			VBucket: vb,
			Key:     []byte(k),
			Cas:     0,
			Opaque:  uint32(i),
			Extras:  []byte{},
			Body:    []byte{}})
		if err != nil {
			return rv, err
		}
	}

	err := client.Transmit(&Request{
		Opcode: NOOP,
		Key:    []byte{},
		Cas:    0,
		Extras: []byte{},
		Body:   []byte{},
		Opaque: terminalOpaque})
	if err != nil {
		return rv, err
	}

	wg.Wait()

	return rv, nil
}

// A function to perform a CAS transform
type CasFunc func(current []byte) []byte

// Perform a CAS transform with the given function.
//
// If the value does not exist, an empty byte string will be sent to f
func (client *Client) CAS(vb uint16, k string, f CasFunc,
	initexp int) (rv *Response, err error) {

	flags := 0
	exp := 0

	for {
		orig, err := client.Get(vb, k)
		if err != nil && orig != nil && orig.Status != KEY_ENOENT {
			return rv, err
		}

		if orig.Status == KEY_ENOENT {
			init := f([]byte{})
			// If it doesn't exist, add it
			resp, err := client.Add(vb, k, 0, initexp, init)
			if err == nil && resp.Status != KEY_EEXISTS {
				return rv, err
			}
			// Copy the body into this response.
			resp.Body = init
			return resp, err
		} else {
			req := &Request{
				Opcode:  SET,
				VBucket: vb,
				Key:     []byte(k),
				Cas:     orig.Cas,
				Opaque:  0,
				Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
				Body:    f(orig.Body)}

			binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
			resp, err := client.Send(req)
			if err == nil {
				return resp, nil
			}
		}
	}
	panic("Unreachable")
}

// Stats returns a slice of these.
type StatValue struct {
	// The stat key
	Key string
	// The stat value
	Val string
}

// Get stats from the server
// use "" as the stat key for toplevel stats.
func (client *Client) Stats(key string) ([]StatValue, error) {
	rv := make([]StatValue, 0, 128)

	req := &Request{
		Opcode:  STAT,
		VBucket: 0,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  918494,
		Extras:  []byte{}}

	err := transmitRequest(client.conn, req)
	if err != nil {
		return rv, err
	}

	for {
		res, err := getResponse(client.conn, client.hdrBuf)
		if err != nil {
			return rv, err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		rv = append(rv, StatValue{
			Key: k,
			Val: string(res.Body),
		})
	}

	return rv, nil
}

// Get the stats from the server as a map
func (client *Client) StatsMap(key string) (map[string]string, error) {
	rv := make(map[string]string)
	st, err := client.Stats(key)
	if err != nil {
		return rv, err
	}
	for _, sv := range st {
		rv[sv.Key] = sv.Val
	}
	return rv, nil
}

// A memcached response
type Response struct {
	// The command opcode of the command that sent the request
	Opcode CommandCode
	// The status of the response
	Status Status
	// The opaque sent in the request
	Opaque uint32
	// The CAS identifier (if applicable)
	Cas uint64
	// Extras, key, and body for this response
	Extras, Key, Body []byte
	// If true, this represents a fatal condition and we should hang up
	Fatal bool
}

// A debugging string representation of this response
func (res Response) String() string {
	return fmt.Sprintf("{Response status=%v keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

// Response as an error.
func (res Response) Error() string {
	return fmt.Sprintf("Response status=%v, msg: %s",
		res.Status, string(res.Body))
}

// Number of bytes this response consumes on the wire.
func (res *Response) Size() int {
	return HDR_LEN + len(res.Extras) + len(res.Key) + len(res.Body)
}
