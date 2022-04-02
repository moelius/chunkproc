package chunkproc

type BufferParam struct {
	Name string
	Size int
}

type BufferParams []BufferParam

type Buffers map[string][]byte

func newByteBuffers(params []BufferParam) Buffers {
	b := Buffers{}
	for _, p := range params {
		b[p.Name] = make([]byte, p.Size)
	}
	return b
}

func (bb Buffers) Get(name string) (buffer []byte, ok bool, err error) {
	b, ok := bb[name]
	return b, ok, nil
}
