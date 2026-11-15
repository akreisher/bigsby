package redblack

import (
	"cmp"
	"fmt"
	"math/rand"
	"testing"
)

func validateRedBlackTree[K cmp.Ordered, V any](node *Node[K, V]) (blackDepth uint64, err error) {
	if node == nil {
		// Black depth of a nil node is 1
		return 1, nil
	}

	left := node.Children[Left]
	right := node.Children[Right]

	if node.color == Red {
		if left != nil && left.color == Red {
			return 0, fmt.Errorf("Red violation")
		}
		if right != nil && right.color == Red {
			return 0, fmt.Errorf("Red violation")
		}
	}

	leftDepth, err := validateRedBlackTree(left)
	if err != nil {
		return 0, err
	}

	rightDepth, err := validateRedBlackTree(right)
	if err != nil {
		return 0, err
	}

	if leftDepth != rightDepth {
		return 0, fmt.Errorf("Black violation")
	}
	return leftDepth, nil
}

func TestInsertRoot(t *testing.T) {
	tree := Tree[int32, int32]{}
	if tree.Root != nil {
		t.Error("Root not nil at init?")
	}

	var key int32 = 12
	var value int32 = 10
	tree.Insert(key, value)

	if tree.Root == nil {
		t.Error("Root is nil after insert?")
	}

	if tree.Root.Key != key {
		t.Errorf("Root key is %d (expected %d)", tree.Root.Key, key)
	}

	if tree.Root.Value != value {
		t.Errorf("Root value is %d (expected %d)", tree.Root.Value, value)
	}
}

func TestManyInsert(t *testing.T) {
	tree := Tree[int32, int32]{}
	N := 1000
	for range N {
		key := rand.Int31()
		value := rand.Int31()

		tree.Insert(key, value)
		found := *tree.Search(key)
		if found != value {
			t.Errorf("Inserted value at key %d is not %d (got %d)", key, value, found)
		}

		// Tree should remain valid after every insert.
		_, err := validateRedBlackTree(tree.Root)
		if err != nil {
			t.Errorf("Invalid tree after insert: %v", err)
		}
	}
}

func TestManyRemove(t *testing.T) {
	tree := Tree[int32, int32]{}
	N := 1000
	keys := make([]int32, N)
	for i := range N {
		key := rand.Int31()
		value := rand.Int31()
		tree.Insert(key, value)
		keys[i] = key
	}

	// Tree should be valid after every and all inserts.
	_, err := validateRedBlackTree(tree.Root)
	if err != nil {
		t.Errorf("Invalid tree after insert: %v", err)
	}

	// Remove keys in random order.
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, key := range keys {
		tree.Remove(key)
		if tree.Search(key) != nil {
			t.Errorf("Could not delete key %d", key)
		}
		// Tree should be valid after every remove.
		_, err := validateRedBlackTree(tree.Root)
		if err != nil {
			t.Errorf("Invalid tree after insert: %v", err)
		}
	}
}
