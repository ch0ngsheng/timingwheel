package timingwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Timer represents a single event. When the Timer expires, the given
// task will be executed.
type Timer struct {
	expiration int64 // in milliseconds
	task       func()

	// The bucket that holds the list to which this timer's element belongs.
	//
	// NOTE: This field may be updated and read concurrently,
	// through Timer.Stop() and Bucket.Flush().
	b unsafe.Pointer // type: *bucket

	// The timer's element.
	// Timer在bucket的timers双向链表中的元素指针
	element *list.Element
}

func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// Stop prevents the Timer from firing. It returns true if the call
// stops the timer, false if the timer has already expired or been stopped.
//
// If the timer t has already expired and the t.task has been started in its own
// goroutine; Stop does not wait for t.task to complete before returning. If the caller
// needs to know whether t.task is completed, it must coordinate with t.task explicitly.
func (t *Timer) Stop() bool {
	stopped := false
	// 使用CAS for-loop无锁的方式将timer从它的bucket中移除
	// getBucket是原子方法，Remove内加了锁
	// 整个Stop方法没有加锁
	// Timer.Stop和bucket.Flush可能并发执行
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		// If b.Remove is called just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// this may fail to remove t due to the change of t's bucket.
		stopped = b.Remove(t)

		// Thus, here we re-get t's possibly new bucket (nil for case 1, or ab (non-nil) for case 2),
		// and retry until the bucket becomes nil, which indicates that t has finally been removed.
	}
	return stopped
}

// bucket timingwheel上的一个格子，包含过期时间及该时间到期的若干timer
type bucket struct {
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers do not ensure it. So we must keep the 64-bit field
	// as the first field of the struct.
	//
	// For more explanations, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// and https://go101.org/article/memory-layout.html.
	expiration int64

	mu sync.Mutex
	// 该bucket下的所有timer的链表
	timers *list.List
}

func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1,
	}
}

// Expiration 原子性地获取bucket的过期时间
func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// SetExpiration 原子性地设置bucket的过期时间，返回和之前设置的时间是否相同
func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// Add 向bucket中添加指定timer
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()

	// 返回的e是双向链表中新增的元素指针
	e := b.timers.PushBack(t)
	t.setBucket(b)
	t.element = e

	b.mu.Unlock()
}

func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		// If remove is called from within t.Stop, and this happens just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// then t.getBucket will return nil for case 1, or ab (non-nil) for case 2.
		// In either case, the returned value does not equal to b.
		return false
	}
	// 从bucket的timers双向链表中移除timer
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

// Remove 从bucket中移除指定timer
func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// Flush 顺序更新bucket中的全部timers
func (b *bucket) Flush(reinsert func(*Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for e := b.timers.Front(); e != nil; {
		next := e.Next()

		t := e.Value.(*Timer)
		b.remove(t)
		// Note that this operation will either execute the timer's task, or
		// insert the timer into another bucket belonging to a lower-level wheel.
		//
		// In either case, no further lock operation will happen to b.mu.
		reinsert(t)

		e = next
	}

	b.SetExpiration(-1)
}
