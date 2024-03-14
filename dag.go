package merkledag

import (
	"fmt"
	"hash"
)

func Add(store KVStore, node Node, h hash.Hash) ([]byte, error) {
	// 根据节点类型进行不同的处理
	switch n := node.(type) {
	case File:
		// 对于文件，我们直接将其字节内容哈希并存储
		h.Write(n.Bytes())
		hashed := h.Sum(nil)
		err := store.Put(hashed, n.Bytes())
		if err != nil {
			return nil, err
		}
		return hashed, nil
	case Dir:
		// 对于目录，我们递归地添加每个子节点，然后哈希它们的哈希并存储
		it := n.It()
		var hashes [][]byte
		for it.Next() {
			hashed, err := Add(store, it.Node(), h)
			if err != nil {
				return nil, err
			}
			hashes = append(hashes, hashed)
		}
		// 将所有子哈希连接并哈希
		for _, hashed := range hashes {
			h.Write(hashed)
		}
		hashed := h.Sum(nil)
		// 我们需要将子哈希列表存储为目录节点的值
		err := store.Put(hashed, flatten(hashes))
		if err != nil {
			return nil, err
		}
		return hashed, nil
	default:
		return nil, fmt.Errorf("unknown node type")
	}
}

// flatten 将一系列字节切片连接成一个单一的字节切片
func flatten(slices [][]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	ret := make([]byte, totalLen)
	var offset int
	for _, s := range slices {
		offset += copy(ret[offset:], s)
	}
	return ret
}
