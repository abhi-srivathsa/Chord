package chord

import (
	"github.com/abhi-srivathsa/chord/modules"
	"hash"
)

//This file includes the API and internal methods for interacting with the Chord ring's Key-Value store.

type Storage interface {
	Get(string) ([]byte, error)
	Put(string, string) error
	Delete(string) error
	Between([]byte, []byte) ([]*models.KV, error)
	MDelete(...string) error
}

func NewMapStore(hashFunc func() hash.Hash) Storage {
	return &mapStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

type mapStore struct {
	data map[string]string
	Hash func() hash.Hash // The Hash function being used

}

func (a *mapStore) hashKey(key string) ([]byte, error) {
	h := a.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

// Given an abitrary node in the ring, get a value from the datastore.
func (a *mapStore) Get(key string) ([]byte, error) {
	val, ok := a.data[key]
	if !ok {
		return nil, ERR_KEY_NOT_FOUND
	}
	return []byte(val), nil
}

func (a *mapStore) Put(key, value string) error {
  if(key == nil){
    return nil, ERR_KEY_NOT_FOUND
  }
	a.data[key] = value
	return nil
}

func (a *mapStore) Delete(key string) error {
	delete(a.data, key)
	return nil
}

func (a *mapStore) Between(from []byte, to []byte) ([]*models.KV, error) {
	vals := make([]*models.KV, 0, 10)
	for k, v := range a.data {
		hashedKey, err := a.hashKey(k)
		if err != nil {
			continue
		}
		if betweenRightIncl(hashedKey, from, to) {
			pair := &models.KV{
				Key:   k,
				Value: v,
			}
			vals = append(vals, pair)
		}
	}
	return vals, nil
}

func (a *mapStore) MDelete(keys ...string) error {
	for _, k := range keys {
		delete(a.data, k)
	}
	return nil
}
