package lrudir

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"unicode"

	"github.com/alexflint/go-filemutex"
)

// These runes can safely appear in filenames on all operaing systems
const safeChars string = "._-"

// These runes can safely appear in filenames on all operaing systems
var isSafe = make(map[rune]bool)

func init() {
	for _, r := range safeChars {
		isSafe[r] = true
	}
}

// Cache represents an on-disk LRU cache.
type Cache struct {
	Dir  string
	Lock *filemutex.Mutex
}

func bytesFromRune(r rune) []byte {
	buf := make([]byte, 16)
	n := binary.PutVarint(buf, int64(r))
	return buf[:n]
}

// escape maps byte slices to unique strings that are valid filenames on all operating
// systems, while attempting to keep the output as close as possible to the input for
// human readability
func escape(key []byte) string {
	var out string
	for _, r := range string(key) {
		switch {
		case unicode.IsLetter(r) || unicode.IsNumber(r) || isSafe[r]:
			out += string(r)
		case r == '/':
			out += "_%_"
		default:
			out += "#" + hex.EncodeToString(bytesFromRune(r))
		}
	}
	return out
}

// Path gets the path for the entry corresponding to the given key. The path is returned
// regardless of whether that entry exists.
func (c *Cache) Path(key []byte) string {
	return filepath.Join(c.Dir, escape(key))
}

// nextPtr gets the path to the file that contains the key that succeeds the given key.
func (c *Cache) nextPtr(key []byte) string {
	return filepath.Join(c.Dir, escape(key)+"~next")
}

// nextPtr gets the path to the file that contains the key that succeeds the given key.
func (c *Cache) prevPtr(key []byte) string {
	return filepath.Join(c.Dir, escape(key)+"~prev")
}

// Keys gets all keys in the cache, sorted from most to least recently used. This is an
// O(N) operation.
func (c *Cache) Keys() ([][]byte, error) {
	var err error
	var key []byte
	var keys [][]byte
	for {
		key, err = ioutil.ReadFile(c.nextPtr(key))
		if err != nil {
			return nil, err
		}
		if len(key) == 0 {
			break
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Get returns the value for the given key
func (c *Cache) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("cannot get the empty key")
	}

	buf, err := ioutil.ReadFile(c.Path(key))
	if err != nil {
		return nil, err
	}

	err = c.detach(key)
	if err != nil {
		return nil, err
	}

	err = c.attachHead(key)
	if err != nil {
		return nil, err
	}

	return buf, err
}

// Put sets the value for the given key
func (c *Cache) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("cannot put the empty key")
	}

	err := ioutil.WriteFile(c.Path(key), value, 0777)
	if err != nil {
		return err
	}

	err = c.detach(key)
	if err != nil && !os.IsNotExist(err) {
		// ignore file-does-not-exist errors since we are inserting a new entry
		return err
	}

	return c.attachHead(key)
}

// Delete removes the given key from the cache
func (c *Cache) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("cannot delete the empty key")
	}

	err := c.detach(key)
	if err != nil {
		return err
	}

	err = os.Remove(c.Path(key))
	if err != nil {
		return err
	}

	err = os.Remove(c.nextPtr(key))
	if err != nil {
		return err
	}

	err = os.Remove(c.prevPtr(key))
	if err != nil {
		return err
	}
	return nil
}

// Oldest gets the oldest key from the cache
func (c *Cache) Oldest() ([]byte, error) {
	return ioutil.ReadFile(c.prevPtr(nil))
}

// DeleteOldest removes the oldest key from the cache
func (c *Cache) DeleteOldest() error {
	key, err := c.Oldest()
	if err != nil {
		return err
	}
	return c.Delete(key)
}

// attachHead attaches the given key at the head of the linked list
func (c *Cache) attachHead(key []byte) error {
	headkey, err := ioutil.ReadFile(c.nextPtr(nil))
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.nextPtr(nil), key, 0777)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.prevPtr(key), nil, 0777)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.nextPtr(key), headkey, 0777)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.prevPtr(headkey), key, 0777)
	if err != nil {
		return err
	}
	return nil
}

// detach removes the given key from the linked list but does not delete the file itself
func (c *Cache) detach(key []byte) error {
	if len(key) == 0 {
		panic(errors.New("cannot detach the empty key"))
	}

	nextkey, err := ioutil.ReadFile(c.nextPtr(key))
	if err != nil {
		return err
	}

	prevkey, err := ioutil.ReadFile(c.prevPtr(key))
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.prevPtr(nextkey), prevkey, 0777)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(c.nextPtr(prevkey), nextkey, 0777)
	if err != nil {
		return err
	}

	return nil
}

// Create initializes an LRU cache in the given directory. The directory
// must already exist.
func Create(path string) (*Cache, error) {
	// Create the lock
	lock, err := filemutex.New(filepath.Join(path, ".lrulock"))
	if err != nil {
		os.RemoveAll(path)
		return nil, err
	}

	// Construct the cache
	c := &Cache{
		Dir:  path,
		Lock: lock,
	}

	// Set the head to nil
	err = ioutil.WriteFile(c.nextPtr(nil), nil, 0777)
	if err != nil {
		return nil, err
	}

	// Set the tail to nil
	err = ioutil.WriteFile(c.prevPtr(nil), nil, 0777)
	if err != nil {
		return nil, err
	}

	// Set the initial state
	var x state
	err = c.setState(&x)
	if err != nil {
		os.RemoveAll(path)
		return nil, err
	}

	return c, nil
}

// Open opens the given directory as an LRU cache. It returns an error if the directory
// does not exist, or if it is not an LRU cache.
func Open(path string) (*Cache, error) {
	// Open the lock
	lock, err := filemutex.New(filepath.Join(path, ".lrulock"))
	if err != nil {
		return nil, err
	}

	// Construct the cache
	c := &Cache{
		Dir:  path,
		Lock: lock,
	}

	// Check that we can read the state
	_, err = c.state()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// OpenOrCreate opens the given directory as an LRU cache, or creates an LRU cache at that
// location if it does not exist. It returns an error if the directory exists but is not
// an LRU cache.
func OpenOrCreate(path string) (*Cache, error) {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return Create(path)
	}
	return Open(path)
}

// state represents information stored in the .lru file
type state struct{}

// load state for an LRU directory
func (c *Cache) state() (*state, error) {
	r, err := os.Open(filepath.Join(c.Dir, ".lru"))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var x state
	dec := json.NewDecoder(r)
	err = dec.Decode(&x)
	if err != nil {
		return nil, err
	}
	return &x, nil
}

// set state for an LRU directory
func (c *Cache) setState(s *state) error {
	w, err := os.Create(filepath.Join(c.Dir, ".lru"))
	if err != nil {
		return err
	}
	defer w.Close()

	enc := json.NewEncoder(w)
	err = enc.Encode(s)
	if err != nil {
		return err
	}
	return nil
}
