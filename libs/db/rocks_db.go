// +build rocksdb

package db

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/stumble/gorocksdb"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewRocksDB(name, dir)
	}
	registerDBCreator(RocksDBBackend, dbCreator, false)
}

var _ DB = (*RocksDB)(nil)

type RocksDB struct {
	db     *gorocksdb.DB
	ro     *gorocksdb.ReadOptions
	wo     *gorocksdb.WriteOptions
	woSync *gorocksdb.WriteOptions

	// options
	cache *gorocksdb.Cache
	env   *gorocksdb.Env
}

func NewRocksDB(name string, dir string) (*RocksDB, error) {
	dbPath := filepath.Join(dir, name+".db")

	/// performance tunings
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	cache := gorocksdb.NewLRUCache(4 << 30)

	// one possible optimization on space amplification, larger block size.
	bbto.SetBlockSize(64 * 1024) // 64KB
	filter := gorocksdb.NewBloomFilter(10)
	bbto.SetFilterPolicy(filter)
	bbto.SetBlockCache(cache)
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)

	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	// Parallelism options
	NumCompactionThread := 8
	env := gorocksdb.NewDefaultEnv()
	// min(the number of cores in the system, the disk throughput
	// divided by the average throughput of one compaction thread)
	env.SetBackgroundThreads(NumCompactionThread)
	env.SetHighPriorityBackgroundThreads(2) // 1 is usually good enough, we set 2 here.
	opts.SetEnv(env)

	// marked as deprecated: https://github.com/facebook/rocksdb/wiki/Thread-Pool
	// as max_background_jobs is suggested. However the go binding has not implemented yet,
	// and these two are still respected.
	opts.SetMaxBackgroundCompactions(NumCompactionThread)
	opts.SetMaxBackgroundFlushes(2)

	// Flushing options
	// maximum 1GB.
	opts.SetWriteBufferSize(256 * 1024 * 1024) // 256MB in memory table
	opts.SetMaxWriteBufferNumber(4)

	// L0 -> L1
	opts.SetLevel0FileNumCompactionTrigger(4)
	opts.SetLevel0SlowdownWritesTrigger(12)
	opts.SetLevel0StopWritesTrigger(20) // 20 * 256MB.
	// opts.SetNumLevels(7)
	opts.SetMaxBytesForLevelBase(1 * 1024 * 1024 * 1024) // 256MB * 4
	opts.SetMaxBytesForLevelMultiplier(8)

	// IO
	opts.SetBytesPerSync(1048576) // suggested by basic tuning wiki.

	// File size
	opts.SetTargetFileSizeBase(128 * 1024 * 1024) // max_bytes_for_level_base / 8

	// Level Style Compaction
	opts.SetLevelCompactionDynamicLevelBytes(true)

	// compression
	// space ZSTDCompression < Zlib < LZ4
	opts.SetCompression(gorocksdb.ZSTDCompression)
	compressOpts := gorocksdb.NewDefaultCompressionOptions()
	compressOpts.MaxDictBytes = 256 * 1024 // Dict max 256KB.
	opts.SetCompressionOptions(compressOpts)

	// fd
	opts.SetMaxOpenFiles(10 * 1024)

	// open db
	db, err := gorocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, err
	}

	// c bindings does not have this option exported,
	// so we have to set it dynamically here.
	// allow up to 10 MB for zstd training.
	db.SetOptions([]string{"zstd_max_train_bytes"}, []string{"10485760"})

	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	woSync := gorocksdb.NewDefaultWriteOptions()
	woSync.SetSync(true)
	database := &RocksDB{
		db:     db,
		ro:     ro,
		wo:     wo,
		woSync: woSync,
		cache:  cache,
		env:    env,
	}
	return database, nil
}

// Implements DB.
func (db *RocksDB) Get(key []byte) []byte {
	key = nonNilBytes(key)
	res, err := db.db.Get(db.ro, key)
	if err != nil {
		res.Free()
		panic(err)
	}
	return moveSliceToBytes(res)
}

// Implements DB.
func (db *RocksDB) Has(key []byte) bool {
	return db.Get(key) != nil
}

// Implements DB.
func (db *RocksDB) Set(key []byte, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.wo, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) SetSync(key []byte, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(db.woSync, key, value)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) Delete(key []byte) {
	key = nonNilBytes(key)
	err := db.db.Delete(db.wo, key)
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *RocksDB) DeleteSync(key []byte) {
	key = nonNilBytes(key)
	err := db.db.Delete(db.woSync, key)
	if err != nil {
		panic(err)
	}
}

func (db *RocksDB) DB() *gorocksdb.DB {
	return db.db
}

// Implements DB.
func (db *RocksDB) Close() {
	db.db.Close()
	db.ro.Destroy()
	db.wo.Destroy()
	db.woSync.Destroy()
	db.env.Destroy()
	db.cache.Destroy()
}

// Implements DB.
func (db *RocksDB) Print() {
	itr := db.Iterator(nil, nil)
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
}

// Implements DB.
func (db *RocksDB) Stats() map[string]string {
	// keys := []string{
	// 	"leveldb.aliveiters",
	// 	"leveldb.alivesnaps",
	// 	"leveldb.blockpool",
	// 	"leveldb.cachedblock",
	// 	"leveldb.num-files-at-level{n}",
	// 	"leveldb.openedtables",
	// 	"leveldb.sstables",
	// 	"leveldb.stats",
	// }

	// stats := make(map[string]string, len(keys))
	// for _, key := range keys {
	// 	str := db.db.PropertyValue(key)
	// 	stats[key] = str
	// }
	// return stats
	return make(map[string]string)
}

//----------------------------------------
// Batch

// Implements DB.
func (db *RocksDB) NewBatch() Batch {
	batch := gorocksdb.NewWriteBatch()
	return &rocksDBBatch{db, batch}
}

type rocksDBBatch struct {
	db    *RocksDB
	batch *gorocksdb.WriteBatch
}

// Implements Batch.
func (mBatch *rocksDBBatch) Set(key, value []byte) {
	mBatch.batch.Put(key, value)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Delete(key []byte) {
	mBatch.batch.Delete(key)
}

// Implements Batch.
func (mBatch *rocksDBBatch) Write() {
	err := mBatch.db.db.Write(mBatch.db.wo, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

// Implements Batch.
func (mBatch *rocksDBBatch) WriteSync() {
	err := mBatch.db.db.Write(mBatch.db.woSync, mBatch.batch)
	if err != nil {
		panic(err)
	}
}

// Implements Batch.
func (mBatch *rocksDBBatch) Close() {
	mBatch.batch.Destroy()
}

//----------------------------------------
// Iterator
// NOTE This is almost identical to db/go_level_db.Iterator
// Before creating a third version, refactor.

func (db *RocksDB) Iterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(itr, start, end, false)
}

func (db *RocksDB) ReverseIterator(start, end []byte) Iterator {
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(itr, start, end, true)
}

var _ Iterator = (*rocksDBIterator)(nil)

type rocksDBIterator struct {
	source     *gorocksdb.Iterator
	start, end []byte
	isReverse  bool
	isInvalid  bool
}

func newRocksDBIterator(source *gorocksdb.Iterator, start, end []byte, isReverse bool) *rocksDBIterator {
	if isReverse {
		if end == nil {
			source.SeekToLast()
		} else {
			source.Seek(end)
			if source.Valid() {
				eoakey := moveSliceToBytes(source.Key()) // end or after key
				if bytes.Compare(end, eoakey) <= 0 {
					source.Prev()
				}
			} else {
				source.SeekToLast()
			}
		}
	} else {
		if start == nil {
			source.SeekToFirst()
		} else {
			source.Seek(start)
		}
	}
	return &rocksDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

func (itr rocksDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr rocksDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if itr.isInvalid {
		return false
	}

	// Panic on DB error.  No way to recover.
	itr.assertNoError()

	// If source is invalid, invalid.
	if !itr.source.Valid() {
		itr.isInvalid = true
		return false
	}

	// If key is end or past it, invalid.
	var start = itr.start
	var end = itr.end
	var key = moveSliceToBytes(itr.source.Key())
	if itr.isReverse {
		if start != nil && bytes.Compare(key, start) < 0 {
			itr.isInvalid = true
			return false
		}
	} else {
		if end != nil && bytes.Compare(end, key) <= 0 {
			itr.isInvalid = true
			return false
		}
	}

	// It's valid.
	return true
}

func (itr rocksDBIterator) Key() []byte {
	itr.assertNoError()
	itr.assertIsValid()
	return moveSliceToBytes(itr.source.Key())
}

func (itr rocksDBIterator) Value() []byte {
	itr.assertNoError()
	itr.assertIsValid()
	return moveSliceToBytes(itr.source.Value())
}

func (itr rocksDBIterator) Next() {
	itr.assertNoError()
	itr.assertIsValid()
	if itr.isReverse {
		itr.source.Prev()
	} else {
		itr.source.Next()
	}
}

func (itr rocksDBIterator) Close() {
	itr.source.Close()
}

func (itr rocksDBIterator) assertNoError() {
	if err := itr.source.Err(); err != nil {
		panic(err)
	}
}

func (itr rocksDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("rocksDBIterator is invalid")
	}
}

// moveSliceToBytes will free the slice and copy out a go []byte
// This function can be applied on *Slice returned from Key() and Value()
// of an Iterator, because they are marked as freed.
func moveSliceToBytes(s *gorocksdb.Slice) []byte {
	defer s.Free()
	if !s.Exists() {
		return nil
	}
	v := make([]byte, len(s.Data()))
	copy(v, s.Data())
	return v
}
