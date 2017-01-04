package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func readSigned(r *bufio.Reader) (int64, int, error) {
	n := uint64(0)
	shift := uint32(0)
	sign := int64(1)
	read := 0
	for {
		b, err := r.ReadByte()
		read += 1
		if err != nil {
			return 0, read, err
		}
		v := b &^ 0x80
		if shift == 0 {
			if v&0x1 != 0 {
				sign = -1
			}
			n = n | (uint64(v) >> 1)
			shift = 6
		} else {
			n = n | (uint64(v) << shift)
			shift += 7
		}
		if b&0x80 == 0 {
			if sign < 0 {
				n += 1
			}
			return sign * int64(n), read, nil
		}
	}
}

func readUnsigned(r *bufio.Reader) (uint64, int, error) {
	n := uint64(0)
	shift := uint32(0)
	read := 0
	for {
		b, err := r.ReadByte()
		read += 1
		if err != nil {
			return 0, read, err
		}
		v := b &^ 0x80
		n = n | (uint64(v) << shift)
		if b&0x80 == 0 {
			return n, read, nil
		}
		shift += 7
	}
}

type stringPair struct {
	Key   string
	Value string
}

type stringsTable struct {
	entries []stringPair
	latest  int
}

func NewStringsTable() *stringsTable {
	return &stringsTable{
		entries: make([]stringPair, 15000),
		latest:  0,
	}
}

func (st *stringsTable) Push(k, v string) {
	if len(k)+len(v) > 250 {
		return
	}
	p := stringPair{
		Key:   k,
		Value: v,
	}
	st.entries[st.latest] = p
	st.latest = (st.latest + 1) % len(st.entries)
}

func (st *stringsTable) Get(n int) (string, string, error) {
	if n < 0 || n > len(st.entries) {
		return "", "", fmt.Errorf("out of bounds: %d", n)
	}
	n = st.latest - n
	if n < 0 {
		n = len(st.entries) + n
	}
	p := st.entries[n]
	return p.Key, p.Value, nil
}

type baseReader struct {
	r       *bufio.Reader
	strings *stringsTable
	read    int
	err     error
}

func NewBaseReader(r io.Reader) *baseReader {
	return &baseReader{
		r:       bufio.NewReader(r),
		strings: NewStringsTable(),
	}
}

func (r *baseReader) Reset() {
	r.strings = NewStringsTable()
}

func (r *baseReader) Err() error {
	return r.err
}

func (r *baseReader) ReadByte() byte {
	if r.err != nil {
		return 0
	}
	b, err := r.r.ReadByte()
	r.err = err
	r.read += 1
	return b
}

func (r *baseReader) Read(buf []byte) int {
	if r.err != nil {
		return 0
	}
	n, err := io.ReadFull(r.r, buf)
	r.err = err
	r.read += n
	return n
}

func (r *baseReader) ReadSigned() int64 {
	if r.err != nil {
		return 0
	}
	n, read, err := readSigned(r.r)
	r.read += read
	r.err = err
	return n
}

func (r *baseReader) ReadUnsigned() uint64 {
	if r.err != nil {
		return 0
	}
	n, read, err := readUnsigned(r.r)
	r.read += read
	r.err = err
	return n
}

func (r *baseReader) ReadString() string {
	k, _ := r.readStrings(true)
	return k
}

func (r *baseReader) ReadStrings() (string, string) {
	return r.readStrings(false)
}

func (r *baseReader) readStrings(single bool) (k string, v string) {
	if r.err != nil {
		return
	}
	b, err := r.r.ReadByte()
	if err != nil {
		r.err = err
		return
	}
	if b == 0 {
		r.read += 1
		buf, err := r.r.ReadSlice(0)
		if err != nil {
			r.err = err
			return
		}
		r.read += len(buf)
		k = string(buf[:len(buf)-1])

		if !single {
			buf, err = r.r.ReadSlice(0)
			if err != nil {
				r.err = err
				return
			}
			r.read += len(buf)
			v = string(buf[:len(buf)-1])
		}
		r.strings.Push(k, v)
	} else {
		r.r.UnreadByte()
		index := r.ReadUnsigned()
		if r.err != nil {
			return
		}
		key, value, err := r.strings.Get(int(index))
		if err != nil {
			r.err = err
			return
		}
		k = key
		v = value
	}
	return
}

func (r *baseReader) Offset() int {
	return r.read
}

func (r *baseReader) Discard(n int) (int, error) {
	n, err := r.r.Discard(n)
	r.read += n
	return n, err
}

func parseHeader(r *baseReader) error {
	h := r.ReadByte()
	if r.Err() != nil || h != 0xff {
		return fmt.Errorf("unexpected header byte: %d, %s", h, r.Err())
	}
	kind := r.ReadByte()
	if r.Err() != nil || kind != 0xe0 {
		return fmt.Errorf("unexpected header section: %x, %s", kind, r.Err())
	}
	l := r.ReadUnsigned()
	if l != 4 {
		return fmt.Errorf("unexpected header section length: %d", l)
	}
	buf := make([]byte, 4)
	r.Read(buf)
	if r.Err() != nil {
		return r.Err()
	}
	if string(buf) != "o5m2" {
		return fmt.Errorf("unexpected o5m type: %s", string(buf))
	}
	return nil
}

type BoundingBox struct {
	X1, Y1, X2, Y2 float64
}

func parseBoundingBox(r *baseReader) (BoundingBox, error) {
	bb := BoundingBox{}
	box := make([]int64, 4)
	for i := range box {
		box[i] = r.ReadSigned()
	}
	bb.X1 = float64(box[0]) / 1e7
	bb.Y1 = float64(box[1]) / 1e7
	bb.X2 = float64(box[2]) / 1e7
	bb.Y2 = float64(box[3]) / 1e7
	return bb, r.Err()
}

type StringPair struct {
	Key   string
	Value string
}

type Ref struct {
	Id   int64
	Type int
	Role string
}

type Metadata struct {
	Version   int
	Timestamp int
	Changeset int
	Uid       string
	Author    string
}

type Node struct {
	Id   int64
	Meta Metadata
	Lon  int64
	Lat  int64
	Tags []StringPair
}

func parseMeta(r *baseReader, prev *Metadata) {
	versionDelta := r.ReadUnsigned()
	// TODO: test behaviour when interleaving entries with and without version
	// information. In particular, what are the previous values used as based
	// for delta encoded fields.
	if versionDelta > 0 {
		prev.Version = int(versionDelta)
		prev.Timestamp += int(r.ReadSigned())
		if prev.Timestamp != 0 {
			prev.Changeset += int(r.ReadSigned())
			prev.Uid, prev.Author = r.ReadStrings()
		}
	} else {
		*prev = Metadata{}
	}
}

func parseTags(r *baseReader, length int, tags []StringPair) ([]StringPair, error) {
	for length > 0 {
		start := r.Offset()
		k, v := r.ReadStrings()
		if r.Err() != nil {
			return nil, fmt.Errorf("could not parse tag: %s", r.Err())
		}
		tags = append(tags, StringPair{
			Key:   k,
			Value: v,
		})
		length -= (r.Offset() - start)
	}
	if length < 0 {
		return nil, fmt.Errorf("overread")
	}
	return tags, nil
}

func parseNode(r *baseReader, length int, prev *Node) error {
	offset := r.Offset()
	prev.Id += r.ReadSigned()
	prev.Tags = prev.Tags[:0]
	parseMeta(r, &prev.Meta)
	// TODO: implement 32-bit overflow behaviour (see o5m spec on wiki)
	prev.Lon += r.ReadSigned()
	prev.Lat += r.ReadSigned()
	remaining := length - (r.Offset() - offset)
	tags, err := parseTags(r, remaining, prev.Tags)
	if err != nil {
		return err
	}
	prev.Tags = tags
	return r.Err()
}

type Way struct {
	Id    int64
	Meta  Metadata
	Nodes []int64
	Tags  []StringPair
}

func parseWay(r *baseReader, length int, prev *Way, nodeId int64) (int64, error) {
	offset := r.Offset()
	prev.Id += r.ReadSigned()
	prev.Nodes = prev.Nodes[:0]
	prev.Tags = prev.Tags[:0]
	parseMeta(r, &prev.Meta)

	nodesLength := int(r.ReadUnsigned())
	for nodesLength > 0 {
		start := r.Offset()
		deltaId := r.ReadSigned()
		if r.Err() != nil {
			return 0, fmt.Errorf("could not parse node id: %s", r.Err())
		}
		nodeId += deltaId
		prev.Nodes = append(prev.Nodes, nodeId)
		end := r.Offset()
		nodesLength -= (end - start)
	}
	if nodesLength < 0 {
		return 0, fmt.Errorf("overread")
	}
	remaining := length - (r.Offset() - offset)
	tags, err := parseTags(r, remaining, prev.Tags)
	if err != nil {
		return 0, err
	}
	prev.Tags = tags
	return nodeId, r.Err()
}

type Relation struct {
	Id   int64
	Meta Metadata
	Refs []Ref
	Tags []StringPair
}

func parseRelation(r *baseReader, length int, prev *Relation, refIds []int64) error {
	offset := r.Offset()
	prev.Id += r.ReadSigned()
	prev.Refs = prev.Refs[:0]
	prev.Tags = prev.Tags[:0]
	parseMeta(r, &prev.Meta)
	refLength := int(r.ReadUnsigned())
	for refLength > 0 {
		start := r.Offset()
		deltaId := r.ReadSigned()
		s := r.ReadString()
		if len(s) < 1 {
			return fmt.Errorf("invalid ref string: %s", s)
		}
		if r.Err() != nil {
			return fmt.Errorf("could not parse reference: %s", r.Err())
		}
		typ := -1
		switch s[:1] {
		case "0":
			typ = 0
		case "1":
			typ = 1
		case "2":
			typ = 2
		}
		if typ < 0 {
			return fmt.Errorf("invalid reference type: %s", s)
		}
		refIds[typ] += deltaId
		prev.Refs = append(prev.Refs, Ref{
			Id:   refIds[typ],
			Type: typ,
			Role: s[1:],
		})
		end := r.Offset()
		refLength -= (end - start)
	}
	if refLength < 0 {
		return fmt.Errorf("overread")
	}
	remaining := length - (r.Offset() - offset)
	tags, err := parseTags(r, remaining, prev.Tags)
	if err != nil {
		return err
	}
	prev.Tags = tags
	return r.Err()
}

const (
	BBoxKind     int = 0xdb
	NodeKind     int = 0x10
	WayKind      int = 0x11
	RelationKind int = 0x12
	ResetKind    int = 0xff
	EndKind      int = 0xfe
)

type O5MReader struct {
	fp   *os.File
	r    *baseReader
	err  error
	kind int

	offset      int
	boundingBox *BoundingBox
	node        Node
	way         Way
	nodeId      int64
	relation    Relation
	refIds      []int64
}

func NewO5MReader(path string) (*O5MReader, error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &O5MReader{
		fp: fp,
		r:  NewBaseReader(fp),
	}
	err = parseHeader(r.r)
	if err != nil {
		return nil, err
	}
	r.reset()
	return r, nil
}

func (r *O5MReader) Close() error {
	return r.fp.Close()
}

func (r *O5MReader) reset() {
	r.node = Node{}
	r.way = Way{}
	r.nodeId = 0
	r.relation = Relation{}
	r.r.Reset()
	r.refIds = make([]int64, 3)
}

func (r *O5MReader) Next() bool {
	for {
		r.offset = r.r.Offset()
		k := r.r.ReadByte()
		if r.r.Err() != nil {
			r.err = fmt.Errorf("cannot read dataset header: %s", r.r.Err())
			return false
		}
		kind := int(k)
		if kind == ResetKind {
			r.reset()
			continue
		}
		if kind == EndKind {
			return false
		}
		r.kind = int(kind)
		l := r.r.ReadUnsigned()
		if r.r.Err() != nil {
			r.err = r.r.Err()
			return false
		}
		length := int(l)
		start := r.r.Offset()
		switch kind {
		case NodeKind:
			err := parseNode(r.r, length, &r.node)
			if err != nil {
				r.err = err
				return false
			}
		case WayKind:
			nodeId, err := parseWay(r.r, length, &r.way, r.nodeId)
			if err != nil {
				r.err = err
				return false
			}
			r.nodeId = nodeId
		case RelationKind:
			err := parseRelation(r.r, length, &r.relation, r.refIds)
			if err != nil {
				r.err = err
				return false
			}
		case BBoxKind:
			bb, err := parseBoundingBox(r.r)
			if err != nil {
				r.err = err
				return false
			}
			r.boundingBox = &bb
		default:
			r.err = fmt.Errorf("unsupported dataset: %x", kind)
			return false
		}
		end := r.r.Offset()
		if (end - start) != length {
			r.err = fmt.Errorf("section length and read data mismatch: %d != %d",
				length, (end - start))
			return false
		}
		return true
	}
}

func (r *O5MReader) Err() error {
	return r.err
}

func (r *O5MReader) Kind() int {
	return r.kind
}

func (r *O5MReader) BoundingBox() BoundingBox {
	if r.kind != BBoxKind {
		panic("not a bounding box")
	}
	return *r.boundingBox
}

func (r *O5MReader) Node() *Node {
	if r.kind != NodeKind {
		panic("not a node")
	}
	return &r.node
}

func (r *O5MReader) Way() *Way {
	if r.kind != WayKind {
		panic("not a way")
	}
	return &r.way
}

func (r *O5MReader) Relation() *Relation {
	if r.kind != RelationKind {
		panic("not a relation")
	}
	return &r.relation
}
