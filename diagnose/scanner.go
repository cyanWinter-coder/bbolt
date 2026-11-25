package diagnose

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"

	"go.etcd.io/bbolt/internal/common"
)

const (
	pageHeaderSize   = 16
	metaSize         = 64
	metaOffset       = pageHeaderSize
	metaChecksumOff  = 56
	branchElemSize   = 16
	leafElemSize     = 16
	bucketHeaderSize = 16
)

const noFreelistPage = uint64(common.PgidNoFreelist)

type rawPage struct {
	id       uint64
	flags    uint16
	count    uint16
	overflow uint32
	data     []byte
	inline   bool
}

type pageRef struct {
	rootPageID uint64
	bucket     [][]byte
	key        *Key
	kind       string
}

type metaInfo struct {
	report MetaReport
	magic  uint32
	valid  bool
	usable bool
}

type scanner struct {
	path     string
	file     *os.File
	fileSize int64
	pageSize uint64
	scanHWM  uint64

	rep *Report

	metas  [2]*metaInfo
	active *metaInfo

	freed     map[uint64]int
	reachable map[uint64]pageRef
	uncertain map[uint64]bool
	visited   map[uint64]bool
	counted   map[uint64]bool

	bucketValid int
	inlineValid int
	freelistOK  int
}

// Diagnose reads path without creating a bbolt transaction or taking a lock.
// The file is opened O_RDONLY and is never modified.
func Diagnose(path string) (*Report, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}

	s := &scanner{
		path:      path,
		file:      f,
		fileSize:  st.Size(),
		freed:     make(map[uint64]int),
		reachable: make(map[uint64]pageRef),
		uncertain: make(map[uint64]bool),
		visited:   make(map[uint64]bool),
		counted:   make(map[uint64]bool),
		rep: &Report{
			SchemaVersion: SchemaVersion,
			Status:        StatusHealthy,
			Database:      path,
			ReadOnly:      true,
			FileSize:      st.Size(),
			Counts: Counts{
				Categories:   defaultCategories(),
				IssuesByCode: map[string]int{},
			},
			MetaPages:         []MetaReport{},
			Issues:            []Issue{},
			BranchStops:       []BranchStop{},
			LeakCheckComplete: true,
		},
	}

	s.loadMetas()
	s.chooseGeometry()
	s.scanFreelist()
	s.scanRootBucket()
	s.scanHighWaterMarkForLeaks()
	s.finalize()

	return s.rep, nil
}

func defaultCategories() map[string]CategoryCount {
	return map[string]CategoryCount{
		"meta":              {},
		"bucket":            {},
		"inline_page":       {},
		"freelist":          {},
		"branch_page":       {},
		"leaf_page":         {},
		"unknown_page":      {},
		"reference":         {},
		"freelist_conflict": {},
		"leak":              {},
		"root":              {},
		"key":               {},
		"page":              {},
		"parse":             {},
	}
}

func (s *scanner) addIssue(iss Issue) {
	if iss.Severity == "" {
		iss.Severity = "error"
	}
	if iss.BucketPathHex == nil {
		iss.BucketPathHex = []string{}
	}
	if iss.FirstBadKey == nil {
		iss.FirstBadKey = iss.Key
	}
	s.rep.Issues = append(s.rep.Issues, iss)
	s.rep.Counts.IssuesByCode[iss.Code]++
	if iss.Category != "" {
		c := s.rep.Counts.Categories[iss.Category]
		c.Issues++
		s.rep.Counts.Categories[iss.Category] = c
	}
}

func uint64Ptr(v uint64) *uint64 { return &v }

func intPtr(v int) *int { return &v }

func pathString(segments [][]byte) string {
	if len(segments) == 0 {
		return "/"
	}
	var b strings.Builder
	for _, seg := range segments {
		b.WriteByte('/')
		b.WriteString(url.PathEscape(string(seg)))
	}
	return b.String()
}

func pathHex(segments [][]byte) []string {
	out := make([]string, 0, len(segments))
	for _, seg := range segments {
		out = append(out, fmt.Sprintf("%x", seg))
	}
	return out
}

func childPath(parent [][]byte, key []byte) [][]byte {
	out := make([][]byte, 0, len(parent)+1)
	out = append(out, parent...)
	out = append(out, append([]byte(nil), key...))
	return out
}

func (s *scanner) stopBranch(pageID *uint64, bucket [][]byte, key *Key, code, category, reason string) {
	hexPath := pathHex(bucket)
	s.rep.BranchStops = append(s.rep.BranchStops, BranchStop{
		PageID:        pageID,
		BucketPath:    pathString(bucket),
		BucketPathHex: hexPath,
		Key:           key,
		FirstBadKey:   key,
		Reason:        reason,
	})
	s.addIssue(Issue{
		Code:          code,
		Category:      category,
		PageID:        pageID,
		BucketPath:    pathString(bucket),
		BucketPathHex: hexPath,
		Key:           key,
		Message:       reason,
	})
}

func (s *scanner) safeBranch(pageID *uint64, bucket [][]byte, key *Key, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			reason := fmt.Sprintf("panic while parsing branch: %v", r)
			s.stopBranch(pageID, bucket, key, "page_unparseable", "parse", reason)
		}
	}()
	fn()
}

func (s *scanner) readAtFull(buf []byte, offset int64) error {
	if offset < 0 || uint64(offset)+uint64(len(buf)) > uint64(s.fileSize) {
		return io.ErrUnexpectedEOF
	}
	n, err := s.file.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func decodeMeta(buf []byte, pageID uint64, offset int64) metaInfo {
	mr := MetaReport{
		PageID:         pageID,
		FileOffset:     offset,
		Present:        len(buf) >= metaOffset+metaSize,
		FailureReasons: []string{},
	}
	mi := metaInfo{report: mr}
	if !mr.Present {
		mr.FailureReasons = append(mr.FailureReasons, fmt.Sprintf("short meta read: %d bytes", len(buf)))
		mi.report = mr
		return mi
	}

	b := buf[metaOffset:]
	mi.magic = binary.NativeEndian.Uint32(b[0:4])
	mr.Magic = fmt.Sprintf("0x%08x", mi.magic)
	mr.MagicOK = mi.magic == common.Magic
	mr.Version = binary.NativeEndian.Uint32(b[4:8])
	mr.VersionOK = mr.Version == common.Version
	mr.PageSize = binary.NativeEndian.Uint32(b[8:12])
	mr.RootPage = binary.NativeEndian.Uint64(b[16:24])
	mr.FreelistPage = binary.NativeEndian.Uint64(b[32:40])
	mr.HighWaterMark = binary.NativeEndian.Uint64(b[40:48])
	mr.TransactionID = binary.NativeEndian.Uint64(b[48:56])
	wantChecksum := binary.NativeEndian.Uint64(b[56:64])

	h := fnv.New64a()
	_, _ = h.Write(b[:metaChecksumOff])
	mr.ChecksumOK = h.Sum64() == wantChecksum

	if !mr.MagicOK {
		mr.FailureReasons = append(mr.FailureReasons, fmt.Sprintf("unexpected magic %s", mr.Magic))
	}
	if !mr.VersionOK {
		mr.FailureReasons = append(mr.FailureReasons, fmt.Sprintf("unsupported version %d", mr.Version))
	}
	if !mr.ChecksumOK {
		mr.FailureReasons = append(mr.FailureReasons, "meta checksum mismatch")
	}
	if mr.PageSize < 512 || mr.PageSize > 16<<20 {
		mr.FailureReasons = append(mr.FailureReasons, fmt.Sprintf("implausible page size %d", mr.PageSize))
	}

	mi.valid = mr.MagicOK && mr.VersionOK && mr.ChecksumOK
	mi.usable = mr.PageSize >= 512 && mr.PageSize <= 16<<20 &&
		mr.HighWaterMark > 0 && mr.HighWaterMark < (1<<48)
	mr.Valid = mi.valid
	mi.report = mr
	return mi
}

func (s *scanner) loadMetas() {
	// Meta page 0 always starts at offset zero.
	first := make([]byte, 4096)
	n, _ := s.file.ReadAt(first, 0)
	m0 := decodeMeta(first[:n], 0, 0)
	s.metas[0] = &m0

	// Build candidate offsets for meta page 1.  A valid m0 page size is
	// preferred; otherwise try the same powers of two used by bbolt itself.
	candidates := []uint64{}
	seen := map[uint64]bool{}
	addCandidate := func(v uint64) {
		if v >= 512 && v <= 16<<20 && !seen[v] {
			seen[v] = true
			candidates = append(candidates, v)
		}
	}
	if m0.report.PageSize != 0 {
		addCandidate(uint64(m0.report.PageSize))
	}
	addCandidate(uint64(os.Getpagesize()))
	for i := 0; i <= 14; i++ {
		addCandidate(uint64(1024) << uint(i))
	}

	var m1 *metaInfo
	for _, off := range candidates {
		if int64(off)+80 > s.fileSize {
			continue
		}
		buf := make([]byte, 4096)
		rn, err := s.file.ReadAt(buf, int64(off))
		if err != nil && !errors.Is(err, io.EOF) {
			continue
		}
		candidate := decodeMeta(buf[:rn], 1, int64(off))
		if m1 == nil {
			cp := candidate
			m1 = &cp
		}
		// Page 1's byte offset is exactly one page size.
		if candidate.valid && uint64(candidate.report.PageSize) == off {
			m1 = &candidate
			break
		}
		// Prefer a raw decode at the meta0 page-size offset if it exists.
		if off == uint64(m0.report.PageSize) {
			cp := candidate
			m1 = &cp
		}
	}
	if m1 == nil {
		m1 = &metaInfo{report: MetaReport{
			PageID:         1,
			Present:        false,
			FailureReasons: []string{"meta page 1 could not be read at a supported page offset"},
		}}
	}
	s.metas[1] = m1

	validCount := 0
	for _, m := range s.metas {
		if m == nil {
			continue
		}
		s.rep.MetaPages = append(s.rep.MetaPages, m.report)
		if m.report.Present {
			s.rep.Counts.Pages.Meta++
		}
		if m.valid {
			validCount++
		}
		for _, reason := range m.report.FailureReasons {
			pageID := m.report.PageID
			s.addIssue(Issue{
				Code:       "meta_invalid",
				Category:   "meta",
				PageID:     uint64Ptr(pageID),
				BucketPath: "",
				Key:        nil,
				Message:    reason,
			})
		}
	}
	c := s.rep.Counts.Categories["meta"]
	c.Scanned = 2
	c.Valid = validCount
	s.rep.Counts.Categories["meta"] = c
}

func (s *scanner) chooseGeometry() {
	var chosen *metaInfo
	if s.metas[0] != nil && s.metas[0].valid {
		chosen = s.metas[0]
	}
	if s.metas[1] != nil && s.metas[1].valid {
		if chosen == nil || s.metas[1].report.TransactionID > chosen.report.TransactionID {
			chosen = s.metas[1]
		}
	}
	if chosen == nil {
		// Prefer a checksum-bad but plausible later meta over giving up.
		if s.metas[1] != nil && s.metas[1].usable {
			chosen = s.metas[1]
		} else if s.metas[0] != nil && s.metas[0].usable {
			chosen = s.metas[0]
		}
	}
	s.active = chosen

	if chosen != nil && chosen.usable {
		s.pageSize = uint64(chosen.report.PageSize)
		s.rep.PageSize = s.pageSize
		s.rep.HighWaterMark = chosen.report.HighWaterMark
		s.scanHWM = s.rep.HighWaterMark
		s.rep.ActiveMetaPage = uint64Ptr(chosen.report.PageID)
		s.rep.ActiveTransaction = uint64Ptr(chosen.report.TransactionID)
		s.rep.PhysicalPages = uint64(s.fileSize) / s.pageSize
		if s.scanHWM > s.rep.PhysicalPages {
			// Report the meta value, but never iterate billions of IDs when a
			// corrupt HWM points beyond the readable file.
			s.scanHWM = s.rep.PhysicalPages
		}
		if rem := uint64(s.fileSize) % s.pageSize; rem != 0 {
			s.addIssue(Issue{
				Code:       "file_partial_page",
				Category:   "meta",
				PageID:     uint64Ptr(s.rep.PhysicalPages),
				BucketPath: "",
				Message:    fmt.Sprintf("file size %d leaves %d trailing bytes against page size %d", s.fileSize, rem, s.pageSize),
			})
		}
		if s.rep.HighWaterMark > s.rep.PhysicalPages {
			s.addIssue(Issue{
				Code:       "meta_hwm_beyond_file",
				Category:   "meta",
				PageID:     nil,
				BucketPath: "",
				Message: fmt.Sprintf("meta high-water mark %d exceeds readable page count %d",
					s.rep.HighWaterMark, s.rep.PhysicalPages),
			})
		}
	} else {
		// Last-resort geometry for a totally unreadable meta region.
		s.pageSize = uint64(os.Getpagesize())
		s.rep.PageSize = s.pageSize
		s.rep.PhysicalPages = uint64(s.fileSize) / s.pageSize
		s.scanHWM = s.rep.PhysicalPages
		s.stopBranch(nil, nil, nil, "meta_unreadable", "meta",
			"no valid or plausible meta page; bucket and freelist traversal was not possible")
	}

	// The fixed meta pages are always reachable.
	s.reachable[0] = pageRef{rootPageID: 0, bucket: nil, kind: "meta"}
	s.reachable[1] = pageRef{rootPageID: 1, bucket: nil, kind: "meta"}
}

func (s *scanner) readPage(id uint64) (*rawPage, error) {
	if s.pageSize == 0 {
		return nil, errors.New("page size is unknown")
	}
	if id > (1<<48)/s.pageSize {
		return nil, fmt.Errorf("page id %d overflows offset calculation", id)
	}
	off := int64(id * s.pageSize)
	hdr := make([]byte, pageHeaderSize)
	if err := s.readAtFull(hdr, off); err != nil {
		return nil, fmt.Errorf("read page header: %w", err)
	}
	p := &rawPage{
		id:       binary.NativeEndian.Uint64(hdr[0:8]),
		flags:    binary.NativeEndian.Uint16(hdr[8:10]),
		count:    binary.NativeEndian.Uint16(hdr[10:12]),
		overflow: binary.NativeEndian.Uint32(hdr[12:16]),
	}
	if p.id != id {
		return nil, fmt.Errorf("page self id %d does not match requested id %d", p.id, id)
	}
	extentPages := uint64(p.overflow) + 1
	if extentPages > s.rep.PhysicalPages && s.rep.PhysicalPages != 0 {
		return nil, fmt.Errorf("page claims %d extent pages but file has only %d pages", extentPages, s.rep.PhysicalPages)
	}
	buf := make([]byte, extentPages*s.pageSize)
	if err := s.readAtFull(buf, off); err != nil {
		return nil, fmt.Errorf("read page body: %w", err)
	}
	p.data = buf
	return p, nil
}

func (p *rawPage) typeName() string {
	switch {
	case p.flags == common.BranchPageFlag:
		return "branch"
	case p.flags == common.LeafPageFlag:
		return "leaf"
	case p.flags == common.MetaPageFlag:
		return "meta"
	case p.flags == common.FreelistPageFlag:
		return "freelist"
	default:
		return fmt.Sprintf("unknown<%04x>", p.flags)
	}
}

func (p *rawPage) branchElement(i int) (key []byte, child uint64, err error) {
	off := pageHeaderSize + i*branchElemSize
	if off < 0 || off+branchElemSize > len(p.data) {
		return nil, 0, fmt.Errorf("branch element %d header is outside page", i)
	}
	e := p.data[off : off+branchElemSize]
	pos := uint64(binary.NativeEndian.Uint32(e[0:4]))
	ksize := uint64(binary.NativeEndian.Uint32(e[4:8]))
	child = binary.NativeEndian.Uint64(e[8:16])
	start := uint64(off) + pos
	end := start + ksize
	if end < start || end > uint64(len(p.data)) {
		return nil, child, fmt.Errorf("branch element %d key range [%d,%d) is outside page", i, start, end)
	}
	key = append([]byte(nil), p.data[start:end]...)
	return key, child, nil
}

func (p *rawPage) leafElement(i int) (flags uint32, key, value []byte, err error) {
	off := pageHeaderSize + i*leafElemSize
	if off < 0 || off+leafElemSize > len(p.data) {
		return 0, nil, nil, fmt.Errorf("leaf element %d header is outside page", i)
	}
	e := p.data[off : off+leafElemSize]
	flags = binary.NativeEndian.Uint32(e[0:4])
	pos := uint64(binary.NativeEndian.Uint32(e[4:8]))
	ksize := uint64(binary.NativeEndian.Uint32(e[8:12]))
	vsize := uint64(binary.NativeEndian.Uint32(e[12:16]))
	start := uint64(off) + pos
	keyEnd := start + ksize
	valueEnd := keyEnd + vsize
	if keyEnd < start || valueEnd < keyEnd || valueEnd > uint64(len(p.data)) {
		return 0, nil, nil, fmt.Errorf("leaf element %d key/value range is outside page", i)
	}
	key = append([]byte(nil), p.data[start:keyEnd]...)
	value = p.data[keyEnd:valueEnd]
	return flags, key, value, nil
}

func (p *rawPage) freelistIDs() ([]uint64, error) {
	if p.flags != common.FreelistPageFlag {
		return nil, fmt.Errorf("not a freelist page: flags=0x%04x", p.flags)
	}
	idx := 0
	count := int(p.count)
	if p.count == 0xffff {
		if len(p.data) < pageHeaderSize+8 {
			return nil, errors.New("truncated oversized freelist count")
		}
		count = int(binary.NativeEndian.Uint64(p.data[pageHeaderSize : pageHeaderSize+8]))
		idx = 1
	}
	start := pageHeaderSize + idx*8
	if start > len(p.data) || count < 0 || count > (len(p.data)-start)/8 {
		return nil, fmt.Errorf("freelist id count %d does not fit in page", count)
	}
	ids := make([]uint64, count)
	for i := range ids {
		off := start + i*8
		ids[i] = binary.NativeEndian.Uint64(p.data[off : off+8])
	}
	return ids, nil
}

func decodeBucketHeader(value []byte) (rootPage uint64, sequence uint64, err error) {
	if len(value) < bucketHeaderSize {
		return 0, 0, fmt.Errorf("bucket value length %d is shorter than bucket header %d", len(value), bucketHeaderSize)
	}
	return binary.NativeEndian.Uint64(value[0:8]), binary.NativeEndian.Uint64(value[8:16]), nil
}

func (s *scanner) claim(p *rawPage, ref pageRef) {
	for i := uint64(0); i <= uint64(p.overflow); i++ {
		id := p.id + i
		current := ref
		current.rootPageID = p.id
		if prev, ok := s.reachable[id]; ok {
			iss := Issue{
				Code:            "page_multiple_references",
				Category:        "reference",
				PageID:          uint64Ptr(id),
				BucketPath:      pathString(ref.bucket),
				BucketPathHex:   pathHex(ref.bucket),
				Key:             ref.key,
				OtherPageID:     uint64Ptr(prev.rootPageID),
				OtherBucketPath: pathString(prev.bucket),
				OtherBucketHex:  pathHex(prev.bucket),
				OtherKey:        prev.key,
				Message: fmt.Sprintf("page %d is referenced more than once; current reference is %s",
					id, ref.kind),
			}
			s.addIssue(iss)
		} else {
			s.reachable[id] = current
		}

		if n := s.freed[id]; n > 0 {
			s.addIssue(Issue{
				Code:          "page_reachable_and_freed",
				Category:      "freelist",
				PageID:        uint64Ptr(id),
				BucketPath:    pathString(ref.bucket),
				BucketPathHex: pathHex(ref.bucket),
				Key:           ref.key,
				Message:       fmt.Sprintf("page %d is reachable from a bucket but occurs %d time(s) in freelist", id, n),
			})
		}
	}
}

func (s *scanner) firstDecodableKey(p *rawPage) *Key {
	if p == nil || p.count == 0 {
		return nil
	}
	if p.flags == common.LeafPageFlag {
		if _, key, _, err := p.leafElement(0); err == nil {
			return NewKey(key)
		}
	}
	if p.flags == common.BranchPageFlag {
		if key, _, err := p.branchElement(0); err == nil {
			return NewKey(key)
		}
	}
	// Damaged type flags do not tell us which element format to use.  Try both
	// and return the first bounded key that can be decoded.
	if _, key, _, err := p.leafElement(0); err == nil {
		return NewKey(key)
	}
	if key, _, err := p.branchElement(0); err == nil {
		return NewKey(key)
	}
	return nil
}

func (s *scanner) recoverPossibleReferences(p *rawPage) {
	if p == nil || p.inline {
		return
	}
	mark := func(id uint64) {
		if id >= 2 && (s.rep.HighWaterMark == 0 || id < s.rep.HighWaterMark) {
			s.uncertain[id] = true
		}
	}

	// A damaged page can lose its type flag while its branch element table is
	// still intact.  Recover those IDs only to avoid calling the children
	// leaked; they are not treated as verified/reachable.
	for i := 0; i < int(p.count); i++ {
		_, child, err := p.branchElement(i)
		if child != 0 {
			mark(child)
		}
		if err != nil && i > int(p.count)/2 {
			break
		}
	}

	// Likewise recover persistent bucket roots from a leaf-shaped page.
	for i := 0; i < int(p.count); i++ {
		flags, _, value, err := p.leafElement(i)
		if err != nil {
			if i > int(p.count)/2 {
				break
			}
			continue
		}
		if flags&common.BucketLeafFlag == 0 || len(value) < bucketHeaderSize {
			continue
		}
		root := binary.NativeEndian.Uint64(value[0:8])
		mark(root)
	}
}

func (s *scanner) countPhysicalPage(p *rawPage) {
	if s.counted[p.id] {
		return
	}
	s.counted[p.id] = true
	s.rep.Counts.Pages.OverflowPages += uint64(p.overflow)
	switch {
	case p.flags == common.BranchPageFlag:
		s.rep.Counts.Pages.Branch++
		c := s.rep.Counts.Categories["branch_page"]
		c.Scanned++
		s.rep.Counts.Categories["branch_page"] = c
	case p.flags == common.LeafPageFlag:
		s.rep.Counts.Pages.Leaf++
		c := s.rep.Counts.Categories["leaf_page"]
		c.Scanned++
		s.rep.Counts.Categories["leaf_page"] = c
	case p.flags == common.FreelistPageFlag:
		s.rep.Counts.Pages.Freelist++
	case p.flags == common.MetaPageFlag:
		s.rep.Counts.Pages.Meta++
	default:
		s.rep.Counts.Pages.Unknown++
		c := s.rep.Counts.Categories["unknown_page"]
		c.Scanned++
		s.rep.Counts.Categories["unknown_page"] = c
	}
}

func (s *scanner) markPageValidCategory(p *rawPage) {
	switch {
	case p.flags == common.BranchPageFlag:
		c := s.rep.Counts.Categories["branch_page"]
		c.Valid++
		s.rep.Counts.Categories["branch_page"] = c
	case p.flags == common.LeafPageFlag:
		c := s.rep.Counts.Categories["leaf_page"]
		c.Valid++
		s.rep.Counts.Categories["leaf_page"] = c
	}
}

func (s *scanner) scanFreelist() {
	fp := s.activeReportFreelist()
	if fp == noFreelistPage {
		s.rep.Counts.Freelist.Persisted = false
		s.rep.Counts.Freelist.Reconstructed = true
		return
	}
	s.rep.Counts.Freelist.Persisted = true
	if s.active == nil || !s.active.usable {
		s.stopBranch(uint64Ptr(fp), nil, nil, "freelist_unreadable", "freelist",
			"no usable meta page provides a freelist page id")
		return
	}
	if fp >= s.rep.HighWaterMark {
		s.addIssue(Issue{
			Code:       "freelist_page_out_of_bounds",
			Category:   "freelist",
			PageID:     uint64Ptr(fp),
			BucketPath: "",
			Message:    fmt.Sprintf("freelist page %d is at/above high-water mark %d", fp, s.rep.HighWaterMark),
		})
		return
	}

	pageID := uint64(fp)
	s.safeBranch(&pageID, nil, nil, func() {
		p, err := s.readPage(pageID)
		if err != nil {
			s.stopBranch(&pageID, nil, nil, "freelist_unreadable", "freelist", err.Error())
			return
		}
		s.countPhysicalPage(p)
		if p.flags != common.FreelistPageFlag {
			s.addIssue(Issue{
				Code:       "freelist_page_invalid_type",
				Category:   "freelist",
				PageID:     uint64Ptr(pageID),
				BucketPath: "",
				Message:    fmt.Sprintf("freelist pointer names %s page, expected freelist", p.typeName()),
			})
			return
		}
		ids, err := p.freelistIDs()
		if err != nil {
			s.stopBranch(&pageID, nil, nil, "freelist_unreadable", "freelist", err.Error())
			return
		}
		s.freelistOK++
		s.rep.Counts.Freelist.Pages = 1 + int(p.overflow)
		s.rep.Counts.Freelist.IDs = len(ids)
		unique := map[uint64]struct{}{}
		for _, id := range ids {
			s.freed[id]++
			if s.freed[id] > 1 {
				var firstKey *Key
				if id >= 2 && (s.rep.HighWaterMark == 0 || id < s.rep.HighWaterMark) {
					if duplicatePage, err := s.readPage(id); err == nil {
						firstKey = s.firstDecodableKey(duplicatePage)
					}
				}
				s.rep.Counts.Freelist.Duplicates++
				s.addIssue(Issue{
					Code:           "freelist_duplicate_page",
					Category:       "freelist",
					PageID:         uint64Ptr(id),
					FreelistPageID: uint64Ptr(pageID),
					BucketPath:     "",
					Key:            firstKey,
					Message:        fmt.Sprintf("page %d appears %d times in freelist page %d", id, s.freed[id], pageID),
				})
			}
			unique[id] = struct{}{}
			if id < 2 || (s.rep.HighWaterMark != 0 && id >= s.rep.HighWaterMark) {
				s.addIssue(Issue{
					Code:       "freelist_page_out_of_bounds",
					Category:   "freelist",
					PageID:     uint64Ptr(id),
					BucketPath: "",
					Message:    fmt.Sprintf("freelist contains page %d outside valid range [2,%d)", id, s.rep.HighWaterMark),
				})
			}
		}
		s.rep.Counts.Freelist.UniqueIDs = len(unique)
		s.claim(p, pageRef{rootPageID: pageID, kind: "freelist"})
		c := s.rep.Counts.Categories["freelist"]
		c.Scanned = s.rep.Counts.Freelist.Pages
		c.Valid = s.freelistOK
		s.rep.Counts.Categories["freelist"] = c
	})
}

func (s *scanner) activeReportFreelist() uint64 {
	if s.active == nil {
		return uint64(common.PgidNoFreelist)
	}
	return s.active.report.FreelistPage
}

func (s *scanner) scanRootBucket() {
	s.rep.Counts.Buckets.Total++
	s.bucketValid++
	if s.active == nil || !s.active.usable {
		s.addIssue(Issue{
			Code:       "root_node_corrupt",
			Category:   "root",
			PageID:     nil,
			BucketPath: "/",
			Message:    "active meta does not provide a usable root bucket page",
		})
		return
	}
	rootID := s.active.report.RootPage
	if rootID < 2 || rootID >= s.rep.HighWaterMark {
		s.addIssue(Issue{
			Code:       "root_node_corrupt",
			Category:   "root",
			PageID:     uint64Ptr(rootID),
			BucketPath: "/",
			Message:    fmt.Sprintf("root bucket page %d is outside [2,%d)", rootID, s.rep.HighWaterMark),
		})
		return
	}
	s.visitPageTree(rootID, nil, nil, nil, nil, pageRef{rootPageID: rootID, bucket: nil, kind: "meta-root"})
}

// visitPageTree parses one bucket B-tree root and all descendants.  minKey and
// maxKey are the half-open range imposed by the parent branch.
func (s *scanner) visitPageTree(id uint64, bucket [][]byte, key, minKey, maxKey []byte, ref pageRef) {
	pageID := id
	s.safeBranch(&pageID, bucket, ref.key, func() {
		alreadyVisited := s.visited[id]
		s.visited[id] = true

		p, err := s.readPage(id)
		if err != nil {
			code := "page_unparseable"
			category := "parse"
			if strings.Contains(err.Error(), "self id") {
				code = "page_id_mismatch"
				category = "page"
				s.addIssue(Issue{
					Code:       code,
					Category:   category,
					PageID:     uint64Ptr(id),
					BucketPath: pathString(bucket),
					Key:        keyFromReport(ref.key),
					Message:    err.Error(),
				})
			} else {
				s.stopBranch(uint64Ptr(id), bucket, ref.key, code, category, err.Error())
			}
			return
		}

		s.countPhysicalPage(p)
		if id < 2 || (s.rep.HighWaterMark != 0 && id >= s.rep.HighWaterMark) {
			s.addIssue(Issue{
				Code:          "page_out_of_bounds",
				Category:      "page",
				PageID:        uint64Ptr(id),
				BucketPath:    pathString(bucket),
				BucketPathHex: pathHex(bucket),
				Key:           ref.key,
				Message:       fmt.Sprintf("page %d is outside high-water mark %d", id, s.rep.HighWaterMark),
			})
		}
		s.claim(p, ref)
		if !p.IsValidType() {
			code := "page_invalid_type"
			category := "page"
			switch ref.kind {
			case "meta-root":
				code = "root_node_corrupt"
				category = "root"
			case "bucket-root":
				code = "bucket_root_corrupt"
				category = "bucket"
			}
			s.addIssue(Issue{
				Code:          code,
				Category:      category,
				PageID:        uint64Ptr(id),
				BucketPath:    pathString(bucket),
				BucketPathHex: pathHex(bucket),
				Key:           ref.key,
				Message:       fmt.Sprintf("page %d has invalid type %s (flags=0x%04x)", id, p.typeName(), p.flags),
			})
			s.recoverPossibleReferences(p)
			s.stopBranch(uint64Ptr(id), bucket, ref.key, "branch_stopped", "parse",
				"cannot traverse a page that is neither branch nor leaf")
			return
		}

		s.markPageValidCategory(p)
		if alreadyVisited {
			// The duplicate reference was reported by claim(); do not walk the
			// same subtree a second time.
			return
		}

		if p.flags == common.BranchPageFlag {
			s.checkBranchPage(p, bucket, minKey, maxKey)
		} else {
			s.checkLeafPage(p, bucket, minKey, maxKey)
		}
	})
}

func keyFromReport(k *Key) *Key { return k }

func (p *rawPage) IsValidType() bool {
	return p.flags == common.BranchPageFlag || p.flags == common.LeafPageFlag
}

func compareBytes(a, b []byte) int {
	return strings.Compare(string(a), string(b))
}

func (s *scanner) addKeyIssue(code, category string, pageID uint64, bucket [][]byte, index int, key *Key, format string, args ...any) {
	s.addIssue(Issue{
		Code:          code,
		Category:      category,
		PageID:        uint64Ptr(pageID),
		BucketPath:    pathString(bucket),
		BucketPathHex: pathHex(bucket),
		Key:           key,
		Index:         intPtr(index),
		Message:       fmt.Sprintf(format, args...),
	})
}

func (s *scanner) checkKeyOrder(pageID uint64, bucket [][]byte, index int, key []byte, previous, minKey, maxKey []byte) []byte {
	reportKey := NewKey(key)
	if index == 0 && minKey != nil && compareBytes(minKey, key) > 0 {
		s.addKeyIssue("key_range_violation", "key", pageID, bucket, index, reportKey,
			"first key is below ancestor lower bound %x", minKey)
	}
	if previous != nil {
		cmp := compareBytes(previous, key)
		if cmp > 0 {
			s.addKeyIssue("key_order_violation", "key", pageID, bucket, index, reportKey,
				"key is smaller than previous key %x", previous)
		} else if cmp == 0 {
			s.addKeyIssue("key_order_violation", "key", pageID, bucket, index, reportKey,
				"duplicate key on page; previous key %x", previous)
		}
	}
	if maxKey != nil && compareBytes(key, maxKey) >= 0 {
		s.addKeyIssue("key_range_violation", "key", pageID, bucket, index, reportKey,
			"key is at or above ancestor upper bound %x", maxKey)
	}
	return append([]byte(nil), key...)
}

func (s *scanner) checkBranchPage(p *rawPage, bucket [][]byte, minKey, maxKey []byte) {
	var previous []byte
	for i := 0; i < int(p.count); i++ {
		key, child, err := p.branchElement(i)
		if err != nil {
			s.recoverPossibleReferences(p)
			s.stopBranch(uint64Ptr(p.id), bucket, nil, "page_unparseable", "parse", err.Error())
			return
		}
		s.checkKeyOrder(p.id, bucket, i, key, previous, minKey, maxKey)
		previous = key

		childMin := key
		var childMax []byte
		if i < int(p.count)-1 {
			nextKey, _, nextErr := p.branchElement(i + 1)
			if nextErr == nil {
				childMax = nextKey
			} else {
				childMax = maxKey
			}
		} else {
			childMax = maxKey
		}

		childID := child
		childKey := NewKey(key)
		childBucket := bucket
		s.safeBranch(&childID, childBucket, childKey, func() {
			if childID < 2 || (s.rep.HighWaterMark != 0 && childID >= s.rep.HighWaterMark) {
				s.addIssue(Issue{
					Code:          "page_out_of_bounds",
					Category:      "page",
					PageID:        uint64Ptr(childID),
					BucketPath:    pathString(bucket),
					BucketPathHex: pathHex(bucket),
					Key:           childKey,
					Index:         intPtr(i),
					Message:       fmt.Sprintf("branch element %d points to page %d outside [2,%d)", i, childID, s.rep.HighWaterMark),
				})
				return
			}
			s.visitPageTree(childID, bucket, key, childMin, childMax, pageRef{
				rootPageID: childID, bucket: bucket, key: childKey, kind: "branch-child",
			})
		})
	}
}

func (s *scanner) checkLeafPage(p *rawPage, bucket [][]byte, minKey, maxKey []byte) {
	var previous []byte
	for i := 0; i < int(p.count); i++ {
		flags, key, value, err := p.leafElement(i)
		if err != nil {
			s.recoverPossibleReferences(p)
			s.stopBranch(uint64Ptr(p.id), bucket, nil, "page_unparseable", "parse", err.Error())
			return
		}
		s.checkKeyOrder(p.id, bucket, i, key, previous, minKey, maxKey)
		previous = key

		if flags&common.BucketLeafFlag == 0 {
			continue
		}

		bucketKey := NewKey(key)
		childPath := childPath(bucket, key)
		s.rep.Counts.Buckets.Total++
		s.safeBranch(uint64Ptr(p.id), childPath, bucketKey, func() {
			rootID, _, err := decodeBucketHeader(value)
			if err != nil {
				s.addIssue(Issue{
					Code:          "bucket_header_invalid",
					Category:      "bucket",
					PageID:        uint64Ptr(p.id),
					BucketPath:    pathString(childPath),
					BucketPathHex: pathHex(childPath),
					Key:           bucketKey,
					Index:         intPtr(i),
					Message:       err.Error(),
				})
				return
			}
			s.bucketValid++
			if rootID == 0 {
				s.rep.Counts.Buckets.Inline++
				s.checkInlineBucket(value[bucketHeaderSize:], childPath, bucketKey)
			} else {
				s.rep.Counts.Buckets.Persistent++
				if rootID < 2 || (s.rep.HighWaterMark != 0 && rootID >= s.rep.HighWaterMark) {
					s.addIssue(Issue{
						Code:          "bucket_root_invalid",
						Category:      "bucket",
						PageID:        uint64Ptr(rootID),
						BucketPath:    pathString(childPath),
						BucketPathHex: pathHex(childPath),
						Key:           bucketKey,
						Message:       fmt.Sprintf("bucket root page %d is outside [2,%d)", rootID, s.rep.HighWaterMark),
					})
					return
				}
				s.visitPageTree(rootID, childPath, key, nil, nil, pageRef{
					rootPageID: rootID, bucket: childPath, key: bucketKey, kind: "bucket-root",
				})
			}
		})
	}
}

func (s *scanner) checkInlineBucket(pageBytes []byte, bucketPath [][]byte, bucketKey *Key) {
	s.rep.Counts.InlinePages++
	if len(pageBytes) < pageHeaderSize {
		s.addIssue(Issue{
			Code:          "inline_page_invalid",
			Category:      "inline_page",
			PageID:        nil,
			BucketPath:    pathString(bucketPath),
			BucketPathHex: pathHex(bucketPath),
			Key:           bucketKey,
			Message:       fmt.Sprintf("inline page is only %d bytes; page header needs %d", len(pageBytes), pageHeaderSize),
		})
		return
	}
	p := &rawPage{
		id:       0,
		flags:    binary.NativeEndian.Uint16(pageBytes[8:10]),
		count:    binary.NativeEndian.Uint16(pageBytes[10:12]),
		overflow: binary.NativeEndian.Uint32(pageBytes[12:16]),
		data:     pageBytes,
		inline:   true,
	}
	if p.overflow != 0 {
		s.addIssue(Issue{
			Code:          "inline_page_invalid",
			Category:      "inline_page",
			PageID:        nil,
			BucketPath:    pathString(bucketPath),
			BucketPathHex: pathHex(bucketPath),
			Key:           bucketKey,
			Message:       fmt.Sprintf("inline page claims %d overflow pages", p.overflow),
		})
		return
	}
	if !p.IsValidType() {
		s.addIssue(Issue{
			Code:          "inline_page_invalid",
			Category:      "inline_page",
			PageID:        nil,
			BucketPath:    pathString(bucketPath),
			BucketPathHex: pathHex(bucketPath),
			Key:           bucketKey,
			Message:       fmt.Sprintf("inline page has invalid type %s (flags=0x%04x)", p.typeName(), p.flags),
		})
		return
	}
	s.inlineValid++
	if p.flags == common.BranchPageFlag {
		s.checkBranchPage(p, bucketPath, nil, nil)
	} else {
		s.checkLeafPage(p, bucketPath, nil, nil)
	}
}

func (s *scanner) scanHighWaterMarkForLeaks() {
	if s.scanHWM == 0 {
		return
	}
	knownLeakedExtentPages := map[uint64]bool{}
	leakedFirstKey := map[uint64]*Key{}
	noPersistedFreelist := s.activeReportFreelist() == noFreelistPage
	s.rep.LeakCheckComplete = len(s.rep.BranchStops) == 0
	confidence := "high"
	if !s.rep.LeakCheckComplete {
		confidence = "low"
	}
	for id := uint64(0); id < s.scanHWM; id++ {
		if _, ok := s.reachable[id]; ok {
			continue
		}
		if s.freed[id] > 0 || s.uncertain[id] {
			continue
		}

		// Try to classify the unreachable extent without letting one bad page
		// interrupt the scan.
		if !knownLeakedExtentPages[id] {
			pageID := id
			var firstKey *Key
			s.safeBranch(&pageID, nil, nil, func() {
				p, err := s.readPage(id)
				if err == nil && p.id == id {
					s.countPhysicalPage(p)
					firstKey = s.firstDecodableKey(p)
					for j := uint64(1); j <= uint64(p.overflow) && id+j < s.scanHWM; j++ {
						knownLeakedExtentPages[id+j] = true
					}
				}
			})
			if firstKey != nil {
				leakedFirstKey[id] = firstKey
			}
		}

		if noPersistedFreelist {
			// In NoFreelistSync mode bbolt reconstructs the free set as the
			// complement of reachable pages; these are not leaks.
			s.rep.Counts.Freelist.ReconstructedIDs++
			continue
		}

		s.addIssue(Issue{
			Code:       "page_leak",
			Category:   "leak",
			PageID:     uint64Ptr(id),
			BucketPath: "",
			Key:        leakedFirstKey[id],
			Confidence: confidence,
			Message:    fmt.Sprintf("page %d is below high-water mark but is neither reachable nor present in freelist", id),
		})
	}
}

func (s *scanner) finalize() {
	s.rep.Counts.Pages.InHighWaterMark = s.scanHWM
	// Fill category validity counts that are easier to derive centrally.
	for name, c := range s.rep.Counts.Categories {
		switch name {
		case "meta":
			// Set during loadMetas.
		case "bucket":
			c.Scanned = s.rep.Counts.Buckets.Total
			c.Valid = s.bucketValid
			s.rep.Counts.Categories[name] = c
		case "inline_page":
			c.Scanned = s.rep.Counts.InlinePages
			c.Valid = s.inlineValid
			s.rep.Counts.Categories[name] = c
		case "freelist":
			if s.rep.Counts.Freelist.Persisted {
				c.Scanned = s.rep.Counts.Freelist.Pages
				c.Valid = s.freelistOK
			} else if s.rep.Counts.Freelist.Reconstructed {
				c.Scanned = s.rep.Counts.Freelist.ReconstructedIDs
				c.Valid = s.rep.Counts.Freelist.ReconstructedIDs
			}
			s.rep.Counts.Categories[name] = c
		}
	}

	// Stable ordering makes human and golden output independent of map order.
	sort.SliceStable(s.rep.Issues, func(i, j int) bool {
		a, b := s.rep.Issues[i], s.rep.Issues[j]
		pageA, pageB := uint64(1<<63), uint64(1<<63)
		if a.PageID != nil {
			pageA = *a.PageID
		}
		if b.PageID != nil {
			pageB = *b.PageID
		}
		if pageA != pageB {
			return pageA < pageB
		}
		if a.Category != b.Category {
			return a.Category < b.Category
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.BucketPath != b.BucketPath {
			return a.BucketPath < b.BucketPath
		}
		ka, kb := "", ""
		if a.Key != nil {
			ka = a.Key.Hex
		}
		if b.Key != nil {
			kb = b.Key.Hex
		}
		return ka < kb
	})

	if len(s.rep.Issues) > 0 || len(s.rep.BranchStops) > 0 {
		s.rep.Status = StatusCorrupt
	}
}
