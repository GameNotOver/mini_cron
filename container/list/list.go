package list

type Element[T any] struct {
	next, prev *Element[T]
	list       *List[T]
	Value      T
}

func (e *Element[T]) Next() *Element[T] {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

func (e *Element[T]) Prev() *Element[T] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

type List[T any] struct {
	root Element[T]
	len  int
}

func New[T any]() *List[T] {
	return new(List[T]).Clear()
}

func (l *List[T]) Clear() *List[T] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	return l
}

func (l *List[T]) Len() int {
	return l.len
}

func (l *List[T]) Front() *Element[T] {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

func (l *List[T]) Back() *Element[T] {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

func (l *List[T]) insert(elem *Element[T], at *Element[T]) *Element[T] {
	elem.prev = at
	elem.next = at.next
	elem.prev.next = elem
	elem.next.prev = elem
	elem.list = l
	l.len++
	return elem
}

func (l *List[T]) insertValue(val T, at *Element[T]) *Element[T] {
	return l.insert(&Element[T]{Value: val}, at)
}

func (l *List[T]) remove(at *Element[T]) {
	at.prev.next = at.next
	at.next.prev = at.prev
	at.next = nil
	at.prev = nil
	at.list = nil
	l.len--
	return
}

func (l *List[T]) move(elem *Element[T], at *Element[T]) {
	if elem == at {
		return
	}

	elem.prev.next = elem.next
	elem.next.prev = elem.prev

	elem.prev = at
	elem.next = at.next
	elem.prev.next = elem
	elem.next.prev = elem

	return
}

func (l *List[T]) Remove(elem *Element[T]) {
	if elem.list == l {
		l.remove(elem)
	}
	return
}

func (l *List[T]) RemoveFront() {
	l.Remove(l.Front())
	return
}

func (l *List[T]) RemoveBack() {
	l.Remove(l.Back())
	return
}

func (l *List[T]) PushFront(val T) *Element[T] {
	elem := l.insertValue(val, &l.root)
	return elem
}

func (l *List[T]) PushBack(val T) *Element[T] {
	elem := l.insertValue(val, l.root.prev)
	return elem
}

func (l *List[T]) InsertBefore(val T, at *Element[T]) *Element[T] {
	if at.list != l {
		return nil
	}
	return l.insertValue(val, at.prev)
}

func (l *List[T]) InsertAfter(val T, at *Element[T]) *Element[T] {
	if at.list != l {
		return nil
	}
	return l.insertValue(val, at)
}

func (l *List[T]) MoveBefore(elem *Element[T], at *Element[T]) {
	if elem.list != l || at.list != l || elem == at {
		return
	}
	l.move(elem, at.prev)
}

func (l *List[T]) MoveAfter(elem *Element[T], at *Element[T]) {
	if elem.list != l || at.list != l || elem == at {
		return
	}
	l.move(elem, at)
}

func (l *List[T]) Empty() bool {
	return l.Len() == 0
}
