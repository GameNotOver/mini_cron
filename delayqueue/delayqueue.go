package delayqueue

import (
	"context"
	"mini_cron/container/heap"
	"sync"
	"sync/atomic"
	"time"
)

type Entry[T any] struct {
	value      T
	expiration time.Time
}

type DelayQueue[T any] struct {
	h        *heap.Heap[*Entry[T]]
	mutex    sync.Mutex
	sleeping int32 // 表示Take()是否正在等待队列不为空或更早到时的元素，0表示Take()没有在等待，1表示Take()在等待
	wakeup   chan struct{}
}

func New[T any]() *DelayQueue[T] {
	return &DelayQueue[T]{
		h: heap.New(nil, func(e1 *Entry[T], e2 *Entry[T]) bool {
			return e1.expiration.Before(e2.expiration)
		}),
		wakeup: make(chan struct{}),
	}
}

// Push 添加延迟元素到队列
func (q *DelayQueue[T]) Push(value T, delay time.Duration) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	entry := &Entry[T]{
		value:      value,
		expiration: time.Now().Add(delay),
	}
	q.h.Push(entry)
	// 唤醒等待的Take()，这里表示新添加的元素到期时间是最早的，或者原来队列为空，因此必须唤醒等待的Take()，因为可以拿到更早期的元素
	if q.h.Peak() == entry {
		if atomic.CompareAndSwapInt32(&q.sleeping, 1, 0) {
			q.wakeup <- struct{}{}
		}
	}
}

// Take 等待直到有元素到期，或者ctx被关闭
func (q *DelayQueue[T]) Take(ctx context.Context) (T, bool) {
	for {
		var timer *time.Timer
		q.mutex.Lock()
		// 有元素
		if !q.h.Empty() {
			entry := q.h.Peak()
			now := time.Now()
			if now.After(entry.expiration) {
				q.h.Pop()
				q.mutex.Unlock()
				return entry.value, true
			}
			// 到期时间，使用time.NewTimer()才能调用stop()，从而释放定时器
			timer = time.NewTimer(entry.expiration.Sub(now))
		}
		// 走到这里表示需要等待了，设置为1告诉Push()在有新元素时需要通知
		atomic.StoreInt32(&q.sleeping, 1)
		q.mutex.Unlock()

		if timer != nil {
			select {
			case <-q.wakeup: // 新的更快到期的元素
				timer.Stop()
			case <-timer.C: // 首元素到期
				// 设置为0，如果原来也为0表示有Push()正在q.wakeup被阻塞
				if atomic.SwapInt32(&q.sleeping, 0) == 0 {
					<-q.wakeup
				}
			case <-ctx.Done(): //被关闭
				timer.Stop()
				var t T
				return t, false
			}
		} else {
			select {
			case <-q.wakeup: // 新的更快到期的元素
			case <-ctx.Done(): //被关闭
				var t T
				return t, false
			}
		}
	}
}

// Channel 返回一个通道，输出到期元素，size表示通道缓存大小
func (q *DelayQueue[T]) Channel(ctx context.Context, size int) <-chan T {
	out := make(chan T, size)
	go func() {
		for {
			entry, ok := q.Take(ctx)
			if !ok {
				close(out)
				return
			}
			out <- entry
		}
	}()
	return out
}

// Peek 获取队头元素
func (q *DelayQueue[T]) Peek() (T, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.h.Empty() {
		var t T
		return t, false
	}
	entry := q.h.Peak()
	return entry.value, true
}

// Pop 获取到期元素
func (q *DelayQueue[T]) Pop() (T, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.h.Empty() {
		var t T
		return t, false
	}
	entry := q.h.Peak()
	// 还没有元素到期
	if time.Now().Before(entry.expiration) {
		var t T
		return t, false
	}
	// 移除元素
	q.h.Pop()
	return entry.value, true
}

// Empty 是否队列为空
func (q *DelayQueue[T]) Empty() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.h.Empty()
}
