package account

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/zenon-network/go-zenon/chain/account/mailbox"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
)

var controlAddress = types.ParseAddressPanic("z1qzal6c5s9rjnnxd2z7dvdhjxpmmj4fmw56a0mz")

// closedLevelDB returns a db.DB wrapper around a LevelDB handle that has
// already been closed, so every Get returns leveldb.ErrClosed.
func closedLevelDB(t *testing.T) db.DB {
	wrapper, raw := db.NewLevelDB(t.TempDir())
	common.FailIfErr(t, raw.Close())
	return wrapper
}

func TestSequencerFrontIndexReadError(t *testing.T) {
	as := NewAccountStore(controlAddress, closedLevelDB(t)).(*accountStore)
	_, err := as.sequencerFrontIndex()
	if err == nil {
		t.Fatal("expected error for closed DB, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read sequencer front index") {
		t.Fatalf("expected read diagnostic, got: %v", err)
	}
	if !errors.Is(err, leveldb.ErrClosed) {
		t.Fatalf("expected errors.Is(err, leveldb.ErrClosed), got: %v", err)
	}
}

func TestSequencerFrontPropagatesReadError(t *testing.T) {
	as := NewAccountStore(controlAddress, closedLevelDB(t))
	mb := mailbox.NewAccountMailbox(controlAddress, db.NewMemDB())
	header, err := as.SequencerFront(mb)
	if err == nil {
		t.Fatalf("expected error, got header %v", header)
	}
	if !strings.Contains(err.Error(), "failed to read sequencer front index") {
		t.Fatalf("expected counter read diagnostic, got: %v", err)
	}
	if !errors.Is(err, leveldb.ErrClosed) {
		t.Fatalf("expected errors.Is(err, leveldb.ErrClosed), got: %v", err)
	}
}

// TestSequencerFrontPropagatesMailboxCorruption verifies that a corrupt
// (too-short) sequencer counter in the mailbox surfaces through
// SequencerFront as a diagnostic error rather than a panic or silent nil.
func TestSequencerFrontPropagatesMailboxCorruption(t *testing.T) {
	memdb := db.NewMemDB()
	mb := mailbox.NewAccountMailbox(controlAddress, memdb)

	// Push a header so the counter is initialised to a valid value.
	header := types.AccountHeader{
		Address: controlAddress,
		HashHeight: types.HashHeight{
			Hash:   types.Hash{2},
			Height: 1,
		},
	}
	mb.SequencerPushBack(header)

	// Corrupt the counter by overwriting it with a value too short for
	// uint64 decoding.  The key is sequencerNumInsertedKey (see
	// chain/account/mailbox/keys.go).
	if err := memdb.Put([]byte{7}, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	as := NewAccountStore(controlAddress, db.NewMemDB())
	got, err := as.SequencerFront(mb)
	if err == nil {
		t.Fatalf("expected error for corrupt mailbox counter, got header %v", got)
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Fatalf("expected 'too short' diagnostic, got: %v", err)
	}
}

// recordingDB wraps a db.DB and counts Put calls so tests can assert
// that a panicking mutator performed no writes.
type recordingDB struct {
	db.DB
	puts int
}

func (r *recordingDB) Put(key, value []byte) error {
	r.puts++
	return r.DB.Put(key, value)
}

// TestSequencerPopFrontCorruptCounterNoWrite verifies that
// SequencerPopFront panics on a corrupt front-index before issuing any
// Put: the read fails, DealWithErr panics, and the recording DB must
// show zero writes.
func TestSequencerPopFrontCorruptCounterNoWrite(t *testing.T) {
	memdb := db.NewMemDB()
	rec := &recordingDB{DB: memdb}

	// Corrupt the front index: too short for uint64 decoding.
	// sequencerLastReceivedKey is defined in chain/account/keys.go.
	if err := memdb.Put(sequencerLastReceivedKey, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	as := NewAccountStore(controlAddress, rec)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on corrupt front index, got none")
		}
		if rec.puts != 0 {
			t.Fatalf("expected 0 Put calls on panic, got %d", rec.puts)
		}
	}()
	as.SequencerPopFront()
}

func TestIdentifierCorruptFrontierEntry(t *testing.T) {
	memdb := db.NewMemDB()
	version := types.HashHeight{Height: 1, Hash: types.Hash{1}}
	common.FailIfErr(t, db.SetFrontier(memdb, version, []byte{0xff, 0xfe, 0xfd}))
	as := NewAccountStore(controlAddress, memdb)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on corrupt frontier entry, got none")
		}
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "account store "+controlAddress.String()) || !strings.Contains(msg, "frontier entry at height 1") {
			t.Fatalf("expected diagnostic naming the account store and entry height, got: %v", msg)
		}
	}()
	as.Identifier()
}
