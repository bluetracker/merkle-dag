package merkledag

import (
	"encoding/json"
	"errors"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) ([]byte, error) {
	var obj *Object
	var err error
	switch node.Type() {
	case FILE:
		file, _ := node.(File)
		obj, err = addFileToStore(file, store, h)
	case DIR:
		dir, _ := node.(Dir)
		obj, err = addDirToStore(dir, store, h)
	default:
		err = errors.New("invalid node type")
	}
	if err != nil {
		return nil, err
	}
	return generateMerkleRoot(obj, h), nil
}

func addFileToStore(file File, store KVStore, h hash.Hash) (*Object, error) {
	obj, err := sliceFile(file, store, h)
	if err != nil {
		return nil, err
	}
	err = marshalAndPut(store, obj, h)
	return obj, err
}

func addDirToStore(dir Dir, store KVStore, h hash.Hash) (*Object, error) {
	obj, err := sliceDir(dir, store, h)
	if err != nil {
		return nil, err
	}
	err = marshalAndPut(store, obj, h)
	return obj, err
}

func generateMerkleRoot(obj *Object, h hash.Hash) []byte {
	jsonMarshal, _ := json.Marshal(obj)
	h.Write(jsonMarshal)
	return h.Sum(nil)
}

func marshalAndPut(store KVStore, obj *Object, h hash.Hash) error {
	jsonMarshal, _ := json.Marshal(obj)
	h.Reset()
	h.Write(jsonMarshal)
	if has, _ := store.Has(h.Sum(nil)); !has {
		store.Put(h.Sum(nil), jsonMarshal)
	}
	return nil
}

func sliceFile(file File, store KVStore, h hash.Hash) (*Object, error) {
	obj := &Object{}
	if len(file.Bytes()) <= 256*1024 {
		obj.Data = file.Bytes()
	} else {
		err := sliceAndPut(file.Bytes(), store, h, obj, 0)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func sliceAndPut(data []byte, store KVStore, h hash.Hash, obj *Object, seedId int) error {
	for seedId < len(data) {
		end := seedId + 256*1024
		if end > len(data) {
			end = len(data)
		}
		chunkData := data[seedId:end]
		blob := Object{
			Links: nil,
			Data:  chunkData,
		}
		err := marshalAndPut(store, &blob, h)
		if err != nil {
			return err
		}
		obj.Links = append(obj.Links, Link{
			Hash: h.Sum(nil),
			Size: len(chunkData),
		})
		obj.Data = append(obj.Data, []byte("blob")...)
		seedId += 256 * 1024
	}
	return nil
}

func sliceDir(dir Dir, store KVStore, h hash.Hash) (*Object, error) {
	treeObject := &Object{}
	iter := dir.It()
	for iter.Next() {
		node := iter.Node()
		var obj *Object
		var err error
		switch node.Type() {
		case FILE:
			file := node.(File)
			obj, err = sliceFile(file, store, h)
			treeObject.Data = append(treeObject.Data, []byte("link")...)
		case DIR:
			subDir := node.(Dir)
			obj, err = sliceDir(subDir, store, h)
			treeObject.Data = append(treeObject.Data, []byte("tree")...)
		}
		if err != nil {
			return nil, err
		}
		treeObject.Links = append(treeObject.Links, Link{
			Hash: generateMerkleRoot(obj, h),
			Name: node.Name(),
			Size: int(node.Size()),
		})
	}
	err := marshalAndPut(store, treeObject, h)
	return treeObject, err
}
