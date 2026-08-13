package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WALEntry is a single append-only log record. Every mutation (increment,
// decrement, reset, merge) is appended before it is applied, so after a
// crash the log can be replayed to reconstruct the lost state.
type WALEntry struct {
	Seq  uint64 `json:"seq"`
	Op   string `json:"op"` // "increment", "decrement", "reset", "merge"
	Node string `json:"node,omitempty"`
	// Delta is set for increment/decrement entries.
	Delta int64 `json:"delta,omitempty"`
	// Positive/Negative/Clock carry the full merged state for merge entries.
	Positive map[string]int64 `json:"positive,omitempty"`
	Negative map[string]int64 `json:"negative,omitempty"`
	Clock    map[string]int64 `json:"clock,omitempty"`
	Ts       int64            `json:"ts"`
}

// WALStore is a file-backed write-ahead log. Entries are appended as JSON
// lines and truncated after a snapshot is taken.
type WALStore struct {
	dir string

	mu   sync.Mutex
	file *os.File
	seq  uint64
}

func NewWALStore(dir string) (*WALStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	return &WALStore{dir: dir}, nil
}

func (w *WALStore) walPath(nodeID string) string {
	return filepath.Join(w.dir, fmt.Sprintf("counter-%s.wal", nodeID))
}

// Append writes a single entry to the log. The entry's sequence number is
// assigned by the store to guarantee ordering across restarts.
func (w *WALStore) Append(nodeID string, op string, delta int64, positive, negative, clock map[string]int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	file, err := w.openLocked(nodeID)
	if err != nil {
		return err
	}

	w.seq++
	entry := WALEntry{
		Seq:      w.seq,
		Op:       op,
		Delta:    delta,
		Positive: positive,
		Negative: negative,
		Clock:    clock,
		Ts:       time.Now().Unix(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	return file.Sync()
}

// Replay reads all entries from the log in order.
func (w *WALStore) Replay(nodeID string) ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := w.walPath(nodeID)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wal open for replay: %w", err)
	}
	defer file.Close()

	entries := make([]WALEntry, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry WALEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("wal replay decode: %w", err)
		}
		if entry.Seq > w.seq {
			w.seq = entry.Seq
		}
		entries = append(entries, entry)
	}

	return entries, scanner.Err()
}

// Truncate drops the log after a snapshot so it does not grow unbounded.
func (w *WALStore) Truncate(nodeID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close the open handle first: on Windows an open file cannot be removed.
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}

	path := w.walPath(nodeID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("wal truncate: %w", err)
	}
	w.seq = 0
	return nil
}

func (w *WALStore) openLocked(nodeID string) (*os.File, error) {
	if w.file != nil {
		return w.file, nil
	}

	file, err := os.OpenFile(w.walPath(nodeID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	w.file = file
	return file, nil
}

func (w *WALStore) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}