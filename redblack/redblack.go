package redblack

import (
	"cmp"
	"fmt"
	"io"
	"iter"
)

type Color bool

const (
	Red, Black Color = true, false
)

type Direction int

const (
	Left Direction = iota
	Right
)

type Node[K cmp.Ordered, V any] struct {
	Parent   *Node[K, V]
	Children [2]*Node[K, V]
	color    Color
	Key      K
	Value    V
}

func (n *Node[K, V]) String() string {
	if n.color == Black {
		return fmt.Sprintf("Black(%v)", n.Key)
	}
	return fmt.Sprintf("Red(%v)", n.Key)

}

type Tree[K cmp.Ordered, V any] struct {
	Root *Node[K, V]
}

func getHeight[K cmp.Ordered, V any](node *Node[K, V]) uint64 {
	if node == nil {
		return 0
	}

	leftHeight := getHeight(node.Children[Left])
	rightHeight := getHeight(node.Children[Right])
	if leftHeight > rightHeight {
		return leftHeight + 1
	} else {
		return rightHeight + 1
	}

}

func (t *Tree[K, V]) Height() uint64 {
	return getHeight(t.Root)
}

func printNode[K cmp.Ordered, V any](node Node[K, V], buffer string) {
	var color string
	if node.color == Red {
		color = "\033[31m"
	} else {
		color = ""
	}
	fmt.Printf("%s+-%s%v\033[0m\n", buffer, color, node.Key)
}

func printSubtree[K cmp.Ordered, V any](node Node[K, V], prfRight string, prfLeft string, buffer string, out io.Writer) {
	if node.Children[Right] != nil {
		printSubtree(*node.Children[Right], "  ", "| ", buffer+prfRight, out)

	}
	printNode(node, buffer)
	if node.Children[Left] != nil {
		printSubtree(*node.Children[Left], "| ", "  ", buffer+prfLeft, out)
	}
}

func (t *Tree[K, V]) Print(out io.Writer) {
	if t.Root == nil {
		fmt.Println("<NIL>")
		return
	}
	printSubtree(*t.Root, "  ", "  ", "", out)
}

func inOrderIter[K cmp.Ordered, V any](node *Node[K, V], yield func(K, V) bool) bool {
	if node == nil {
		return true
	}
	if node.Children[Left] != nil {
		if !inOrderIter(node.Children[Left], yield) {
			return false
		}
	}
	if !yield(node.Key, node.Value) {
		return false
	}
	if node.Children[Right] != nil {
		if !inOrderIter(node.Children[Right], yield) {
			return false
		}
	}
	return true
}

func (t *Tree[K, V]) InOrder() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		inOrderIter(t.Root, yield)
	}
}

func (n *Node[K, V]) Direction() Direction {
	if n == n.Parent.Children[Left] {
		return Left
	} else {
		return Right
	}
}

func rotateSubtree[K cmp.Ordered, V any](tree *Tree[K, V], sub *Node[K, V], dir Direction) *Node[K, V] {
	subParent := sub.Parent
	newRoot := sub.Children[1-dir]
	newChild := newRoot.Children[dir]

	sub.Children[1-dir] = newChild

	if newChild != nil {
		newChild.Parent = sub
	}

	newRoot.Children[dir] = sub

	newRoot.Parent = subParent
	sub.Parent = newRoot

	if subParent != nil {
		var dir Direction
		if sub == subParent.Children[Right] {
			dir = Right
		}
		subParent.Children[dir] = newRoot
	} else {
		tree.Root = newRoot
	}

	return newRoot
}

func searchInOrderHelper[K cmp.Ordered, V any](node *Node[K, V], key K) *Node[K, V] {
	for node != nil {
		if key > node.Key {
			if node.Children[Right] == nil {
				return node
			} else {
				node = node.Children[Right]
			}
		} else if key < node.Key {
			if node.Children[Left] == nil {
				return node
			} else {
				node = node.Children[Left]
			}
		} else {
			return node
		}
	}
	return nil
}

func (t *Tree[K, V]) Search(key K) *V {
	node := searchInOrderHelper(t.Root, key)
	if node != nil && node.Key == key {
		return &node.Value
	}
	return nil
}

func (t *Tree[K, V]) Insert(key K, value V) {
	if t.Root == nil {
		node := Node[K, V]{Key: key, Value: value, color: Red}
		t.Root = &node
		return
	}

	// Search for node in tree already or get in-order parent.
	parent := searchInOrderHelper(t.Root, key)
	if parent.Key == key {
		// Overwrite value if key exists in tree.
		parent.Value = value
		return
	}

	var dir Direction
	if parent.Key < key {
		dir = Right
	} else {
		dir = Left
	}

	node := Node[K, V]{
		Parent: parent,
		color:  Red,
		Key:    key,
		Value:  value,
	}
	parent.Children[dir] = &node
	curr := &node

	// Rebalance loop
	for parent != nil {
		// Case 1 - Parent is Black. In this case, we can just insert the red
		// node, as adding cannot introduce a red or black violation.
		if parent.color == Black {
			return
		}

		grandparent := parent.Parent
		if grandparent == nil {
			// Case 4 - Parent is the root, we can just change it to black and increase the
			// black height of the tree by one.
			parent.color = Black
			return
		}

		dir = parent.Direction()
		uncle := grandparent.Children[1-dir]
		if uncle == nil || uncle.color == Black {
			// Case 5/6 - Parent is red, but uncle is black.
			// We want to rotate parent to the grandparent position, so that we can
			// swap their colors.

			if curr == parent.Children[1-dir] {
				// Case 5 - Node is inner grand-child.
				// In this case, we must first rotate about the parent and re-assign
				// the current node so that we can force it to be an outer grand-child.
				rotateSubtree(t, parent, dir)
				curr = parent
				parent = grandparent.Children[dir]
			}

			// Case 6 - Node is now guaranteed an outer grand-child.
			// We rotate about the grandparent so that the parent is now in the
			// grandparent position, and swap their color.
			rotateSubtree(t, grandparent, 1-dir)
			parent.color = Black
			grandparent.color = Red
			return
		}

		// Case 2 - Both the parent and uncle are red.
		// In this case, we can "push the black" down from the grandparent
		// to the parent/uncle. We must then recurse with node set to the
		// grandparent as we may have introduced a red violation between the
		// grandparent and its parent.
		parent.color = Black
		uncle.color = Black
		grandparent.color = Red
		curr = grandparent
		parent = curr.Parent
	}
	// Case 3 - We have executed Case 2 up the tree, and the current node is the root.
	// Nothing else to do at this point.
}

func removeBlackLeafNode[K cmp.Ordered, V any](tree *Tree[K, V], node *Node[K, V]) {
	var sibling *Node[K, V]
	var distantNephew *Node[K, V]
	var closeNephew *Node[K, V]

	// Parent should be non-nil because this should not be a root node.
	dir := node.Direction()
	parent := node.Parent
	parent.Children[dir] = nil
	node.Parent = nil

	for parent != nil {
		sibling = parent.Children[1-dir]
		distantNephew = sibling.Children[1-dir]
		closeNephew = sibling.Children[dir]

		if sibling.color == Red {
			// Case 3 -- Sibling is red.
			// In this case, nephews must both be black.
			// We can rotate to make the sibling the grandparent of node,
			// Then, after swapping colors of parent and sibling, node has
			// a red parent, so we can apply case 4, 5 or 6 to fix tree.
			rotateSubtree(tree, parent, dir)
			parent.color = Red
			sibling.color = Black
			sibling = closeNephew

			distantNephew = sibling.Children[1-dir]
			if distantNephew != nil && distantNephew.color == Red {
				goto case6
			}

			closeNephew = sibling.Children[dir]
			if closeNephew != nil && closeNephew.color == Red {
				goto case5
			}

			// Case 4
			sibling.color = Red
			parent.color = Black
			return
		}

		if distantNephew != nil && distantNephew.color == Red {
			goto case6
		}

		if closeNephew != nil && closeNephew.color == Red {
			goto case5
		}

		if parent.color == Red {
			// Case 4 - Sibling and nephews are black, parent is red.
			// We can exchange parent and sibling colors. This does not
			// affect black depth going through sibling's paths, but adds
			// but makes up for deleted node paths.
			sibling.color = Red
			parent.color = Black
			return
		}

		// Case 2 -- Parent, sibling, and nephews are all black.
		// In this case, we can recolor the sibling red to maintain
		// same black depth on both parent's paths. This reduces the black depth
		// by 1, so we need to reassign node to the parent to maybe fix a level up.
		sibling.color = Red
		node = parent
		parent = node.Parent
		if parent != nil {
			dir = node.Direction()
		} else {
			// Case 1 -- Current node is the new root.
			// One black level was removed from every path
			// so no black violations.
			return
		}
	}

case5:
	// Case 5 -- Sibling, and distant nephew are black, close nephew is red.
	// We can rotate the sibling in the opposite direction to make the close
	// nephew the new sibling, and the old sibling the distant nephew. We swap
	// colors between the old sibling and close nephew, so that the node now
	// has a black sibling and red distant nephew, so case 6 can be applied.
	rotateSubtree(tree, sibling, 1-dir)
	sibling.color = Red
	closeNephew.color = Black
	distantNephew = sibling
	sibling = closeNephew
case6:
	// Case 6 -- Sibling is black, distant nephew is red.
	// Rotate about parent to make sibling the grandparent of node.
	// Swap colors.
	rotateSubtree(tree, parent, dir)
	sibling.color = parent.color
	parent.color = Black
	distantNephew.color = Black
}

func removeNode[K cmp.Ordered, V any](tree *Tree[K, V], node *Node[K, V]) {
	parent := node.Parent

	// Simple cases
	// 1. Node has two children
	if node.Children[Left] != nil && node.Children[Right] != nil {
		rightNode := node.Children[Right]

		for rightNode.Children[Left] != nil {
			rightNode = rightNode.Children[Left]
		}

		// Copy key/value of successor into node to delete, and
		// delete the successor instead.
		node.Key = rightNode.Key
		node.Value = rightNode.Value
		removeNode(tree, rightNode)
		return
	}

	// 2. Deleted node has 1 child.
	if node.Children[Left] != nil || node.Children[Right] != nil {
		var child *Node[K, V]
		if node.Children[Left] != nil {
			child = node.Children[Left]
		} else {
			child = node.Children[Right]
		}
		if parent != nil {
			parent.Children[node.Direction()] = child
		}
		child.Parent = parent
		node.Parent = nil
		child.color = Black

		// If node was the root, replace.
		if tree.Root == node {
			tree.Root = child
		}

		return
	}

	// 3. Node has no children and is the root.
	if tree.Root == node {
		tree.Root = nil
		return
	}

	// 4. Node has no children and is red.
	if node.color == Red {
		parent.Children[node.Direction()] = nil
		node.Parent = nil
		return
	}

	removeBlackLeafNode(tree, node)
}

func (t *Tree[K, V]) Remove(key K) {
	node := searchInOrderHelper(t.Root, key)
	// Not in tree
	if node == nil || node.Key != key {
		return
	}
	removeNode(t, node)
}
