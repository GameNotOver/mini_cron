package timingwheel

import (
	"mini_cron/container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

type Timer struct {
	expiration int64
	task       func()
	b          unsafe.Pointer
	elem       *list.Element[*Timer]
}

func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

type bucket struct {
	expiration int64              // 到期时间
	timers     *list.List[*Timer] // 定时器双向链表
	mutex      sync.Mutex
}

func newBucket() *bucket {
	return &bucket{
		expiration: -1,
		timers:     list.New[*Timer](),
	}
}

func (b *bucket) add(t *Timer) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	elem := b.timers.PushBack(t)
	t.elem = elem
	t.setBucket(b)
}

func (b *bucket) remove(t *Timer) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if t.getBucket() != b {
		return false
	}
	b.timers.Remove(t.elem)
	t.setBucket(nil)
	t.elem = nil
	return true
}

// 添加到上一级计时器或执行任务
func (b *bucket) flush(addOrRun func(t *Timer)) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for elem := b.timers.Front(); elem != nil; {
		next := elem.Next()
		t := elem.Value
		if t.getBucket() == b {
			t.setBucket(nil)
			t.elem = nil
		}
		addOrRun(t)
		elem = next
	}
	// 设置过期时间表示没有加入到延迟队列
	b.setExpiration(-1)
	b.timers.Clear()
}

func (b *bucket) getExpiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

func (b *bucket) setExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}
