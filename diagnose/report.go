// Package diagnose implements bounded, read-only physical inspection of a
// bbolt database file.
//
// It intentionally does not open the database through bbolt itself.  That lets
// it produce a report when a meta page or a B-tree page is corrupt and avoids
// taking bbolt's file lock.  Every read is bounds checked; malformed pages stop
// only the branch that contains them.
package diagnose

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
)

const SchemaVersion = 1

const (
	StatusHealthy = "healthy"
	StatusCorrupt = "corrupt"
)

// Key is a lossless and human-friendly representation of a database key.
type Key struct {
	Hex  string `json:"hex"`
	Text string `json:"text,omitempty"`
}

// Issue is one diagnosed inconsistency.
//
// PageID, BucketPath and Key are always present in JSON.  When a value is not
// applicable or cannot be decoded, it is null/empty rather than omitted.
type Issue struct {
	Code            string   `json:"code"`
	Category        string   `json:"category"`
	Severity        string   `json:"severity"`
	PageID          *uint64  `json:"page_id"`
	BucketPath      string   `json:"bucket_path"`
	BucketPathHex   []string `json:"bucket_path_hex"`
	Key             *Key     `json:"key"`
	FirstBadKey     *Key     `json:"first_bad_key"`
	OtherPageID     *uint64  `json:"other_page_id,omitempty"`
	OtherBucketPath string   `json:"other_bucket_path,omitempty"`
	OtherBucketHex  []string `json:"other_bucket_path_hex,omitempty"`
	OtherKey        *Key     `json:"other_key,omitempty"`
	FreelistPageID  *uint64  `json:"freelist_page_id,omitempty"`
	Index           *int     `json:"index,omitempty"`
	Confidence      string   `json:"confidence,omitempty"`
	Message         string   `json:"message"`
}

// BranchStop records a branch that could not be parsed safely.
type BranchStop struct {
	PageID        *uint64  `json:"page_id"`
	BucketPath    string   `json:"bucket_path"`
	BucketPathHex []string `json:"bucket_path_hex"`
	Key           *Key     `json:"key"`
	FirstBadKey   *Key     `json:"first_bad_key"`
	Reason        string   `json:"reason"`
}

// MetaReport describes one of the two fixed meta pages.
type MetaReport struct {
	PageID         uint64   `json:"page_id"`
	FileOffset     int64    `json:"file_offset"`
	Present        bool     `json:"present"`
	Valid          bool     `json:"valid"`
	Magic          string   `json:"magic,omitempty"`
	MagicOK        bool     `json:"magic_ok"`
	Version        uint32   `json:"version,omitempty"`
	VersionOK      bool     `json:"version_ok"`
	ChecksumOK     bool     `json:"checksum_ok"`
	PageSize       uint32   `json:"page_size,omitempty"`
	RootPage       uint64   `json:"root_page,omitempty"`
	FreelistPage   uint64   `json:"freelist_page,omitempty"`
	HighWaterMark  uint64   `json:"high_water_mark,omitempty"`
	TransactionID  uint64   `json:"transaction_id,omitempty"`
	FailureReasons []string `json:"failure_reasons"`
}

// CategoryCount contains coverage and problem counts for one logical category.
type CategoryCount struct {
	Scanned int `json:"scanned"`
	Valid   int `json:"valid"`
	Issues  int `json:"issues"`
}

// PageCounts contains physical page type counts.
type PageCounts struct {
	InHighWaterMark uint64 `json:"in_high_water_mark"`
	Meta            int    `json:"meta"`
	Branch          int    `json:"branch"`
	Leaf            int    `json:"leaf"`
	Freelist        int    `json:"freelist"`
	Unknown         int    `json:"unknown"`
	OverflowPages   uint64 `json:"overflow_pages"`
}

// BucketCounts contains bucket coverage counts.
type BucketCounts struct {
	Total      int `json:"total"`
	Persistent int `json:"persistent"`
	Inline     int `json:"inline"`
}

// FreelistCounts contains freelist coverage counts.
type FreelistCounts struct {
	Persisted        bool `json:"persisted"`
	Reconstructed    bool `json:"reconstructed_by_scan"`
	Pages            int  `json:"pages"`
	IDs              int  `json:"ids"`
	UniqueIDs        int  `json:"unique_ids"`
	Duplicates       int  `json:"duplicate_occurrences"`
	ReconstructedIDs int  `json:"reconstructed_ids"`
}

// Counts is the collection of stable counts in a report.
type Counts struct {
	Categories   map[string]CategoryCount `json:"categories"`
	Pages        PageCounts               `json:"pages"`
	Buckets      BucketCounts             `json:"buckets"`
	InlinePages  int                      `json:"inline_pages"`
	Freelist     FreelistCounts           `json:"freelist"`
	IssuesByCode map[string]int           `json:"issues_by_code"`
}

// Report is the machine-readable diagnostic result.
type Report struct {
	SchemaVersion     int          `json:"schema_version"`
	Status            string       `json:"status"`
	Database          string       `json:"database"`
	ReadOnly          bool         `json:"read_only"`
	FileSize          int64        `json:"file_size"`
	PageSize          uint64       `json:"page_size"`
	HighWaterMark     uint64       `json:"high_water_mark"`
	PhysicalPages     uint64       `json:"physical_pages"`
	ActiveMetaPage    *uint64      `json:"active_meta_page"`
	ActiveTransaction *uint64      `json:"active_transaction_id"`
	Counts            Counts       `json:"counts"`
	LeakCheckComplete bool         `json:"leak_check_complete"`
	MetaPages         []MetaReport `json:"meta_pages"`
	Issues            []Issue      `json:"issues"`
	BranchStops       []BranchStop `json:"branch_stops"`
}

// NewKey converts raw key bytes into the report representation.
func NewKey(k []byte) *Key {
	if k == nil {
		return nil
	}
	out := &Key{Hex: fmt.Sprintf("%x", k)}
	if strings.ToValidUTF8(string(k), "") == string(k) {
		out.Text = string(k)
	}
	return out
}

// JSON writes the stable JSON representation.
func (r *Report) JSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(r)
}

// Text writes a human-readable report.
func (r *Report) Text(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "bbolt diagnostic report\n")
	fmt.Fprintf(tw, "database:\t%s\n", r.Database)
	fmt.Fprintf(tw, "status:\t%s\n", strings.ToUpper(r.Status))
	fmt.Fprintf(tw, "read-only:\t%t\n", r.ReadOnly)
	fmt.Fprintf(tw, "file size:\t%d bytes\n", r.FileSize)
	if r.PageSize != 0 {
		fmt.Fprintf(tw, "page size:\t%d bytes\n", r.PageSize)
	}
	if r.HighWaterMark != 0 {
		fmt.Fprintf(tw, "high-water mark:\t%d\n", r.HighWaterMark)
	}
	if r.ActiveMetaPage != nil {
		fmt.Fprintf(tw, "active meta page:\t%d\n", *r.ActiveMetaPage)
	}
	if r.ActiveTransaction != nil {
		fmt.Fprintf(tw, "active txid:\t%d\n", *r.ActiveTransaction)
	}
	tw.Flush()

	fmt.Fprintln(w, "\nCoverage by category:")
	cats := make([]string, 0, len(r.Counts.Categories))
	for name := range r.Counts.Categories {
		cats = append(cats, name)
	}
	sort.Strings(cats)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, name := range cats {
		c := r.Counts.Categories[name]
		fmt.Fprintf(tw, "  %s:\tscanned=%d\tvalid=%d\tissues=%d\n", name, c.Scanned, c.Valid, c.Issues)
	}
	tw.Flush()

	fmt.Fprintf(w, "\nPages: meta=%d branch=%d leaf=%d freelist=%d unknown=%d overflow=%d\n",
		r.Counts.Pages.Meta, r.Counts.Pages.Branch, r.Counts.Pages.Leaf,
		r.Counts.Pages.Freelist, r.Counts.Pages.Unknown, r.Counts.Pages.OverflowPages)
	fmt.Fprintf(w, "Buckets: total=%d persistent=%d inline=%d inline-pages=%d\n",
		r.Counts.Buckets.Total, r.Counts.Buckets.Persistent, r.Counts.Buckets.Inline, r.Counts.InlinePages)
	if r.LeakCheckComplete {
		fmt.Fprintln(w, "Leak classification: complete (all reachable branches parsed)")
	} else {
		fmt.Fprintln(w, "Leak classification: partial (some branches stopped; low-confidence leak candidates marked)")
	}
	if r.Counts.Freelist.Persisted {
		fmt.Fprintf(w, "Freelist: pages=%d ids=%d unique=%d duplicate-occurrences=%d\n",
			r.Counts.Freelist.Pages, r.Counts.Freelist.IDs, r.Counts.Freelist.UniqueIDs, r.Counts.Freelist.Duplicates)
	} else if r.Counts.Freelist.Reconstructed {
		fmt.Fprintf(w, "Freelist: not persisted; scan reconstructs %d free page ids\n", r.Counts.Freelist.ReconstructedIDs)
	} else {
		fmt.Fprintln(w, "Freelist: no persisted freelist page")
	}

	if len(r.MetaPages) > 0 {
		fmt.Fprintln(w, "\nMeta pages:")
		for _, m := range r.MetaPages {
			state := "valid"
			if !m.Valid {
				state = "INVALID"
			}
			fmt.Fprintf(w, "  page %d: %s txid=%d root=%d freelist=%d hwm=%d checksum=%t\n",
				m.PageID, state, m.TransactionID, m.RootPage, m.FreelistPage, m.HighWaterMark, m.ChecksumOK)
			for _, reason := range m.FailureReasons {
				fmt.Fprintf(w, "    reason: %s\n", reason)
			}
		}
	}

	if len(r.Issues) == 0 {
		fmt.Fprintln(w, "\nNo issues found.")
	} else {
		fmt.Fprintf(w, "\nIssues (%d):\n", len(r.Issues))
		for i, issue := range r.Issues {
			page := "-"
			if issue.PageID != nil {
				page = fmt.Sprint(*issue.PageID)
			}
			key := "-"
			if issue.Key != nil {
				if issue.Key.Text != "" {
					key = fmt.Sprintf("%s (hex=%s)", issue.Key.Text, issue.Key.Hex)
				} else {
					key = "hex=" + issue.Key.Hex
				}
			}
			bucket := issue.BucketPath
			if bucket == "" {
				bucket = "-"
			}
			fmt.Fprintf(w, "  %d. [%s/%s] page=%s bucket=%s key=%s\n",
				i+1, issue.Category, issue.Code, page, bucket, key)
			if issue.Confidence != "" {
				fmt.Fprintf(w, "     confidence: %s\n", issue.Confidence)
			}
			fmt.Fprintf(w, "     %s\n", issue.Message)
			if issue.OtherPageID != nil {
				otherBucket := issue.OtherBucketPath
				if otherBucket == "" {
					otherBucket = "-"
				}
				fmt.Fprintf(w, "     already referenced by page=%d bucket=%s\n", *issue.OtherPageID, otherBucket)
			}
		}
	}

	if len(r.BranchStops) > 0 {
		fmt.Fprintf(w, "\nStopped branches (%d); other branches continued:\n", len(r.BranchStops))
		for _, stop := range r.BranchStops {
			page := "-"
			if stop.PageID != nil {
				page = fmt.Sprint(*stop.PageID)
			}
			bucket := stop.BucketPath
			if bucket == "" {
				bucket = "-"
			}
			fmt.Fprintf(w, "  page=%s bucket=%s reason=%s\n", page, bucket, stop.Reason)
		}
	}

	if r.Status == StatusHealthy {
		fmt.Fprintln(w, "\nRESULT: OK")
	} else {
		fmt.Fprintln(w, "\nRESULT: CORRUPT")
	}
	return nil
}
