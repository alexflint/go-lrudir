package lrudir

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPutDelete(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fmt.Println(dir)

	c, err := Create(dir)
	require.NoError(t, err)

	err = c.Put([]byte("foo"), []byte("bar"))
	require.NoError(t, err)

	val, err := c.Get([]byte("foo"))
	require.NoError(t, err)
	assert.Equal(t, []byte("bar"), val)

	err = c.Delete([]byte("foo"))
	require.NoError(t, err)

	_, err = c.Get([]byte("foo"))
	require.Error(t, err)
}

func TestPutThree(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fmt.Println(dir)

	c, err := Create(dir)
	require.NoError(t, err)

	k1, k2, k3 := []byte("key1"), []byte("key2"), []byte("key3")

	err = c.Put(k1, nil)
	require.NoError(t, err)

	err = c.Put(k2, nil)
	require.NoError(t, err)

	err = c.Put(k3, nil)
	require.NoError(t, err)

	keys, err := c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k3, k2, k1}, keys)

	oldest, err := c.Oldest()
	require.NoError(t, err)
	assert.EqualValues(t, k1, oldest)

	err = c.DeleteOldest()
	require.NoError(t, err)

	keys, err = c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k3, k2}, keys)

	err = c.DeleteOldest()
	require.NoError(t, err)

	keys, err = c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k3}, keys)

	err = c.DeleteOldest()
	require.NoError(t, err)

	keys, err = c.Keys()
	require.NoError(t, err)
	assert.Len(t, keys, 0)
}

func TestReorder(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fmt.Println(dir)

	c, err := Create(dir)
	require.NoError(t, err)

	k1, k2, k3 := []byte("key1"), []byte("key2"), []byte("key3")

	err = c.Put(k1, nil)
	require.NoError(t, err)

	err = c.Put(k2, nil)
	require.NoError(t, err)

	err = c.Put(k3, nil)
	require.NoError(t, err)

	keys, err := c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k3, k2, k1}, keys)

	_, err = c.Get(k1)
	require.NoError(t, err)

	keys, err = c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k1, k3, k2}, keys)

	_, err = c.Get(k3)
	require.NoError(t, err)

	keys, err = c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k3, k1, k2}, keys)

	_, err = c.Get(k3)
	require.NoError(t, err)

	keys, err = c.Keys()
	require.NoError(t, err)
	assert.EqualValues(t, [][]byte{k3, k1, k2}, keys)
}
