package memcache

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestEncodingRequest(t *testing.T) {
	req := Request{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	got := req.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func BenchmarkEncodingRequest(b *testing.B) {
	req := Request{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest0CAS(b *testing.B) {
	req := Request{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest1Extra(b *testing.B) {
	req := Request{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func TestCommandCodeStringin(t *testing.T) {
	if GET.String() != "GET" {
		t.Fatalf("Expected \"GET\" for GET, got \"%v\"", GET.String())
	}

	cc := CommandCode(0x80)
	if cc.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", cc.String())
	}
}

func TestStatusNameString(t *testing.T) {
	if SUCCESS.String() != "SUCCESS" {
		t.Fatalf("Expected \"SUCCESS\" for SUCCESS, got \"%v\"",
			SUCCESS.String())
	}

	s := Status(0x80)
	if s.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", s.String())
	}
}

func TestTransmitReq(t *testing.T) {
	b := bytes.NewBuffer([]byte{})
	buf := bufio.NewWriter(b)

	req := Request{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	err := transmitRequest(buf, &req)
	if err != nil {
		t.Fatalf("Error transmitting request: %v", err)
	}

	buf.Flush()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(b.Bytes()) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", req.Size(),
			len(b.Bytes()))
	}

	if !reflect.DeepEqual(b.Bytes(), expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, b.Bytes())
	}
}

func BenchmarkTransmitReq(b *testing.B) {
	bout := bytes.NewBuffer([]byte{})

	req := Request{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriterSize(bout, req.Size()*2)
		err := transmitRequest(buf, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}

func BenchmarkTransmitReqLarge(b *testing.B) {
	bout := bytes.NewBuffer([]byte{})

	req := Request{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    make([]byte, 24*1024),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriterSize(bout, req.Size()*2)
		err := transmitRequest(buf, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}

func BenchmarkTransmitReqNull(b *testing.B) {
	req := Request{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		err := transmitRequest(ioutil.Discard, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}

/*
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| 0x81          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
      4| 0x04          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
      8| 0x00          | 0x00          | 0x00          | 0x09          |
       +---------------+---------------+---------------+---------------+
     12| 0x00          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
     16| 0x00          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
     20| 0x00          | 0x00          | 0x00          | 0x01          |
       +---------------+---------------+---------------+---------------+
     24| 0xde          | 0xad          | 0xbe          | 0xef          |
       +---------------+---------------+---------------+---------------+
     28| 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    | 0x6c ('l')    |
       +---------------+---------------+---------------+---------------+
     32| 0x64 ('d')    |
       +---------------+

   Field        (offset) (value)
   Magic        (0)    : 0x81
   Opcode       (1)    : 0x00
   Key length   (2,3)  : 0x0000
   Extra length (4)    : 0x04
   Data type    (5)    : 0x00
   Status       (6,7)  : 0x0000
   Total body   (8-11) : 0x00000009
   Opaque       (12-15): 0x00000000
   CAS          (16-23): 0x0000000000000001
   Extras              :
     Flags      (24-27): 0xdeadbeef
   Key                 : None
   Value        (28-32): The textual string "World"

*/

func TestDecodeSpecSample(t *testing.T) {
	data := []byte{
		0x81, 0x00, 0x00, 0x00, // 0
		0x04, 0x00, 0x00, 0x00, // 4
		0x00, 0x00, 0x00, 0x09, // 8
		0x00, 0x00, 0x00, 0x00, // 12
		0x00, 0x00, 0x00, 0x00, // 16
		0x00, 0x00, 0x00, 0x01, // 20
		0xde, 0xad, 0xbe, 0xef, // 24
		0x57, 0x6f, 0x72, 0x6c, // 28
		0x64, // 32
	}

	buf := make([]byte, HDR_LEN)
	res, err := getResponse(bytes.NewReader(data), buf)
	if err != nil {
		t.Fatalf("Error parsing response: %v", err)
	}

	expected := &Response{
		Opcode: GET,
		Status: 0,
		Opaque: 0,
		Cas:    1,
		Extras: []byte{0xde, 0xad, 0xbe, 0xef},
		Key:    []byte{},
		Body:   []byte("World"),
		Fatal:  false,
	}

	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("Expected\n%#v -- got --\n%#v", expected, res)
	}

}

func TestDecode(t *testing.T) {
	data := []byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	buf := make([]byte, HDR_LEN)
	res, err := getResponse(bytes.NewReader(data), buf)
	if err != nil {
		t.Fatalf("Error parsing response: %v", err)
	}

	expected := &Response{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
		Fatal:  false,
	}

	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("Expected\n%#v -- got --\n%#v", expected, res)
	}
}

func BenchmarkDecodeResponse(b *testing.B) {
	data := []byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}
	buf := make([]byte, HDR_LEN)
	b.SetBytes(int64(len(buf)))

	for i := 0; i < b.N; i++ {
		_, err := getResponse(bytes.NewReader(data), buf)
		if err != nil {
			b.Fatalf("Error decoding:  %v", err)
		}
	}
}
