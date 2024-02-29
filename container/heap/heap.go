package heap

type LessFunc[T any] func(e1 T, e2 T) bool

type Heap[T any] struct {
	h        []T
	lessFunc LessFunc[T]
}

func New[T any](h []T, lessFunc LessFunc[T]) *Heap[T] {
	heap := &Heap[T]{
		h:        h,
		lessFunc: lessFunc,
	}
	heap.init()
	return heap
}

func (h *Heap[T]) Len() int {
	return len(h.h)
}

func (h *Heap[T]) Empty() bool {
	return h.Len() == 0
}

func (h *Heap[T]) Pop() T {
	n := h.Len() - 1
	h.swap(0, n)
	h.down(0, n)
	return h.pop()
}

// Peek 获取堆顶元素
func (h *Heap[T]) Peek() T {
	return h.h[0]
}

func (h *Heap[T]) Push(elem T) {
	h.push(elem)
	h.up(h.Len() - 1)
}

func (h *Heap[T]) Remove(idx int) T {
	lastIdx := h.Len() - 1
	if lastIdx != idx {
		h.swap(idx, lastIdx)
		if !h.down(idx, lastIdx) {
			h.up(idx)
		}
	}
	return h.pop()
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
func (h *Heap[T]) Fix(idx int) {
	if !h.down(idx, h.Len()) {
		h.up(idx)
	}
}

func (h *Heap[T]) init() {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *Heap[T]) down(idx int, n int) bool {
	tempParentIdx := idx // idx: parent index
	for {
		tempLeftChildIdx := 2*tempParentIdx + 1
		if tempLeftChildIdx >= n || tempLeftChildIdx < 0 {
			break
		}
		leftChildIdx := tempLeftChildIdx
		if tempRightChildIdx := tempLeftChildIdx + 1; tempRightChildIdx < n && h.less(tempRightChildIdx, tempLeftChildIdx) {
			leftChildIdx = tempRightChildIdx
		}
		if !h.less(leftChildIdx, tempParentIdx) {
			break
		}
		h.swap(tempParentIdx, leftChildIdx)
		tempParentIdx = leftChildIdx
	}
	return tempParentIdx > idx
}

func (h *Heap[T]) up(idx int) {
	// idx: left or right child index
	for {
		parentIdx := (idx - 1) / 2
		if parentIdx == idx || !h.less(idx, parentIdx) {
			break
		}
		h.swap(parentIdx, idx)
		idx = parentIdx
	}
}

func (h *Heap[T]) less(i int, j int) bool {
	return h.lessFunc(h.h[i], h.h[j])
}

func (h *Heap[T]) swap(i int, j int) {
	h.h[i], h.h[j] = h.h[j], h.h[i]
}

func (h *Heap[T]) pop() T {
	lastIdx := h.Len() - 1
	elem := h.h[lastIdx]
	h.h = h.h[:lastIdx]
	return elem
}

func (h *Heap[T]) push(elem T) {
	h.h = append(h.h, elem)
}
