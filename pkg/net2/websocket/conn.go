package websocket

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"

	log "github.com/zeromicro/go-zero/core/logx"
)

const (
	// Frame header byte 0 bits from Section 5.2 of RFC 6455
	finBit  = 1 << 7
	rsv1Bit = 1 << 6
	rsv2Bit = 1 << 5
	rsv3Bit = 1 << 4
	opBit   = 0x0f

	// Frame header byte 1 bits from Section 5.2 of RFC 6455
	maskBit = 1 << 7
	lenBit  = 0x7f

	continuationFrame        = 0
	continuationFrameMaxRead = 1000
)

// The message types are defined in RFC 6455, section 11.8.
const (
	// TextMessage denotes a text data message. The text message payload is
	// interpreted as UTF-8 encoded text data.
	TextMessage = 1

	// BinaryMessage denotes a binary data message.
	BinaryMessage = 2

	// CloseMessage denotes a close control message. The optional message
	// payload contains a numeric code and text. Use the FormatCloseMessage
	// function to format a close message payload.
	CloseMessage = 8

	// PingMessage denotes a ping control message. The optional message payload
	// is UTF-8 encoded text.
	PingMessage = 9

	// PongMessage denotes a ping control message. The optional message payload
	// is UTF-8 encoded text.
	PongMessage = 10
)

var (
	// ErrMessageClose close control message
	ErrMessageClose = errors.New("close control message")
	// ErrMessageMaxRead continuation frame max read
	ErrMessageMaxRead = errors.New("continuation frame max read")
)

// Conn represents a WebSocket connection.
type Conn struct {
	net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	maskKey []byte
	rBuf    []byte
}

// new connection
func newConn(rwc net.Conn, r *bufio.Reader, w *bufio.Writer) *Conn {
	return &Conn{Conn: rwc, r: r, w: w, maskKey: make([]byte, 4)}
}

func (c *Conn) Read(p []byte) (n int, err error) {
	for {
		if len(c.rBuf) >= len(p) {
			copy(p, c.rBuf[:len(p)])
			c.rBuf = c.rBuf[len(p):]
			n = len(p)
			break
		}

		var (
			b []byte
			// op int
		)
		if _, b, err = c.ReadMessage(); err != nil {
			return
		} else if len(b) == 0 {
			// break
			continue
		}

		c.rBuf = append(c.rBuf, b...)
	}
	return
}

func (c *Conn) Write(p []byte) (n int, err error) {
	if err = c.WriteHeader(BinaryMessage, len(p)); err == nil {
		if err = c.WriteBody(p); err == nil {
			n = len(p)
			err = c.Flush()
		}
	}
	return
}

// WriteMessage write a message by type.
func (c *Conn) WriteMessage(msgType int, msg []byte) (err error) {
	if err = c.WriteHeader(msgType, len(msg)); err == nil {
		err = c.WriteBody(msg)
	}
	return
}

// WriteHeader write header frame.
func (c *Conn) WriteHeader(msgType int, length int) (err error) {
	var h2 [10]byte
	//if h, err = c.w.Peek(2); err != nil {
	//	return
	//}
	// 1.First byte. FIN/RSV1/RSV2/RSV3/OpCode(4bits)
	h2[0] = 0
	h2[0] |= finBit | byte(msgType)
	// 2.Second byte. Mask/Payload len(7bits)
	h2[1] = 0
	switch {
	case length <= 125:
		// 7 bits
		h2[1] |= byte(length)
		err = c.WriteBody(h2[:2])
		log.Infof("write: %s", hex.EncodeToString(h2[:2]))
	case length < 65536:
		// 16 bits
		h2[1] |= 126
		//if h, err = c.w.Peek(2); err != nil {
		//	return
		//}
		binary.BigEndian.PutUint16(h2[2:], uint16(length))
		err = c.WriteBody(h2[:4])
		log.Infof("write: %s", hex.EncodeToString(h2[:4]))
	default:
		// 64 bits
		h2[1] |= 127
		//if h, err = c.w.Peek(8); err != nil {
		//	return
		//}
		binary.BigEndian.PutUint64(h2[2:], uint64(length))
		err = c.WriteBody(h2[:10])
		log.Infof("write: %s", hex.EncodeToString(h2[:10]))
	}
	return
}

// WriteBody write a message body.
func (c *Conn) WriteBody(b []byte) (err error) {
	if len(b) > 0 {
		_, err = c.w.Write(b)
		log.Infof("write: %s", hex.EncodeToString(b))
	}
	return
}

// Flush flush writer buffer
func (c *Conn) Flush() error {
	return c.w.Flush()
}

// ReadMessage read a message.
func (c *Conn) ReadMessage() (op int, payload []byte, err error) {
	var (
		fin         bool
		finOp, n    int
		partPayload []byte
	)
	for {
		// read frame
		if fin, op, partPayload, err = c.readFrame(); err != nil {
			log.Errorf("readMessage error: %v", err)
			return
		}
		log.Infof("readFrame: {fin: %v, op: %d, partPayload: %d}", fin, op, len(partPayload))
		switch op {
		case BinaryMessage, TextMessage, continuationFrame:
			if fin && len(payload) == 0 {
				log.Infof("fin && len(payload) == 0")
				return op, partPayload, nil
			}
			// continuation frame
			payload = append(payload, partPayload...)
			if op != continuationFrame {
				finOp = op
			}
			// final frame
			if fin {
				op = finOp
				log.Infof("fin")
				return
			}
		case PingMessage:
			// handler ping
			if err = c.WriteMessage(PongMessage, partPayload); err != nil {
				return
			}
		case PongMessage:
			// handler pong
		case CloseMessage:
			// handler close
			err = ErrMessageClose
			return
		default:
			err = fmt.Errorf("unknown control message, fin=%t, op=%d", fin, op)
			return
		}
		if n > continuationFrameMaxRead {
			err = ErrMessageMaxRead
			return
		}
		n++
	}
}

func (c *Conn) readFrame() (fin bool, op int, payload []byte, err error) {
	var (
		b          byte
		p          []byte
		mask       bool
		maskKey    []byte
		payloadLen int64
	)
	// 1.First byte. FIN/RSV1/RSV2/RSV3/OpCode(4bits)
	b, err = c.r.ReadByte()
	if err != nil {
		return
	}
	// final frame
	fin = (b & finBit) != 0
	// rsv MUST be 0
	if rsv := b & (rsv1Bit | rsv2Bit | rsv3Bit); rsv != 0 {
		return false, 0, nil, fmt.Errorf("unexpected reserved bits rsv1=%d, rsv2=%d, rsv3=%d", b&rsv1Bit, b&rsv2Bit, b&rsv3Bit)
	}
	// op code
	op = int(b & opBit)
	// 2.Second byte. Mask/Payload len(7bits)
	b, err = c.r.ReadByte()
	if err != nil {
		return
	}
	// is mask payload
	mask = (b & maskBit) != 0
	// payload length
	switch b & lenBit {
	case 126:
		// 16 bits
		p = make([]byte, 2)
		if _, err = c.r.Read(p); err != nil {
			return
		}
		payloadLen = int64(binary.BigEndian.Uint16(p))
	case 127:
		// 64 bits
		p = make([]byte, 8)
		if _, err = c.r.Read(p); err != nil {
			return
		}
		payloadLen = int64(binary.BigEndian.Uint64(p))
	default:
		// 7 bits
		payloadLen = int64(b & lenBit)
	}
	// read mask key
	if mask {
		maskKey = make([]byte, 4)
		_, err = c.r.Read(maskKey)
		if err != nil {
			return
		}
		if c.maskKey == nil {
			c.maskKey = make([]byte, 4)
		}
		copy(c.maskKey, maskKey)
	}
	// read payload
	if payloadLen > 0 {
		payload = make([]byte, int(payloadLen))
		if _, err = c.r.Read(payload); err != nil {
			return
		}
		if mask {
			maskBytes(c.maskKey, 0, payload)
		}
	}
	return
}

//// Close close the connection.
//func (c *Conn) Close() error {
//	return c.Conn.Close()
//}

func maskBytes(key []byte, pos int, b []byte) int {
	for i := range b {
		b[i] ^= key[pos&3]
		pos++
	}
	return pos & 3
}

func (c *Conn) Peek(n int) ([]byte, error) {
	for {
		if len(c.rBuf) >= n {
			return c.rBuf[:n], nil
		}

		if _, b, err := c.ReadMessage(); err != nil {
			return nil, err
		} else {
			c.rBuf = append(c.rBuf, b...)
		}
		log.Infof("%d", len(c.rBuf))
	}
}

func (c *Conn) PeekByte() (uint8, error) {
	if b, err := c.Peek(1); err != nil {
		return 0, err
	} else {
		return b[0], nil
	}
}

func (c *Conn) PeekUint32() (uint32, error) {
	if buf, err := c.Peek(4); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint32(buf), nil
	}
}

func (c *Conn) Discard(n int) (int, error) {
	if len(c.rBuf) >= n {
		c.rBuf = c.rBuf[n:]
	}
	return n, nil
}
