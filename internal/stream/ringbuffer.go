package stream

import "sync"

type RingBuffer struct {
	buf    []byte
	size   int
	write  int
	mu     sync.RWMutex
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		buf:  make([]byte, size),
		size: size,
	}
}

func (rb *RingBuffer) Write(data []byte) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for _, b := range data {
		rb.buf[rb.write%rb.size] = b
		rb.write++
	}
}

func (rb *RingBuffer) Read(offset int64, buf []byte) (int, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	totalWritten := rb.write
	if offset >= int64(totalWritten) {
		return 0, nil
	}

	startPos := int(offset % int64(rb.size))
	available := int(int64(totalWritten) - offset)
	if available > len(buf) {
		available = len(buf)
	}

	for i := 0; i < available; i++ {
		buf[i] = rb.buf[(startPos+i)%rb.size]
	}

	return available, nil
}

func (rb *RingBuffer) WritePosition() int64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return int64(rb.write)
}
