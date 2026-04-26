package worker

import (
	"io"
	"sync"
	"time"
)

// LogEvent represents a single log chunk from a running task.
type LogEvent struct {
	Stream string    `json:"stream"` // "stdout" or "stderr"
	Data   string    `json:"data"`
	Time   time.Time `json:"ts"`
}

// LogBroadcaster manages per-task log streams, fanning out written data to
// all active subscribers. It is safe for concurrent use.
type LogBroadcaster struct {
	mu    sync.Mutex
	tasks map[string]*taskStream
}

type taskStream struct {
	mu   sync.Mutex
	subs []subscriber
}

type subscriber struct {
	ch chan LogEvent
	id int
}

// NewLogBroadcaster creates a LogBroadcaster.
func NewLogBroadcaster() *LogBroadcaster {
	return &LogBroadcaster{
		tasks: make(map[string]*taskStream),
	}
}

// Subscribe registers a subscriber for a task's log events. Returns a
// channel that receives events and an unsubscribe function. The channel
// is closed when Unsubscribe is called or when Close(taskID) is called.
func (lb *LogBroadcaster) Subscribe(taskID string, bufSize int) (<-chan LogEvent, func()) {
	lb.mu.Lock()
	ts, ok := lb.tasks[taskID]
	if !ok {
		ts = &taskStream{}
		lb.tasks[taskID] = ts
	}
	lb.mu.Unlock()

	ch := make(chan LogEvent, bufSize)

	ts.mu.Lock()
	id := len(ts.subs) // simple incrementing ID
	ts.subs = append(ts.subs, subscriber{ch: ch, id: id})
	ts.mu.Unlock()

	unsub := func() {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		for i, s := range ts.subs {
			if s.id == id && s.ch == ch {
				ts.subs = append(ts.subs[:i], ts.subs[i+1:]...)
				close(ch)
				return
			}
		}
	}

	return ch, unsub
}

// HasSubscribers returns true if the given task has any active subscribers.
func (lb *LogBroadcaster) HasSubscribers(taskID string) bool {
	lb.mu.Lock()
	ts, ok := lb.tasks[taskID]
	lb.mu.Unlock()
	if !ok {
		return false
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.subs) > 0
}

// Writer returns an io.Writer that broadcasts all written data to
// subscribers of the given task and stream name ("stdout" or "stderr").
// Writes never block — if a subscriber's buffer is full, the event is dropped
// for that subscriber.
func (lb *LogBroadcaster) Writer(taskID, stream string) io.Writer {
	return &logWriter{lb: lb, taskID: taskID, stream: stream}
}

// Close signals that a task's log stream is done. All subscriber channels
// are closed and the task is removed from the broadcaster.
func (lb *LogBroadcaster) Close(taskID string) {
	lb.mu.Lock()
	ts, ok := lb.tasks[taskID]
	if ok {
		delete(lb.tasks, taskID)
	}
	lb.mu.Unlock()

	if ts == nil {
		return
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, s := range ts.subs {
		close(s.ch)
	}
	ts.subs = nil
}

// logWriter implements io.Writer, broadcasting to all subscribers.
type logWriter struct {
	lb     *LogBroadcaster
	taskID string
	stream string
}

func (w *logWriter) Write(p []byte) (int, error) {
	ev := LogEvent{
		Stream: w.stream,
		Data:   string(p),
		Time:   time.Now(),
	}

	w.lb.mu.Lock()
	ts, ok := w.lb.tasks[w.taskID]
	w.lb.mu.Unlock()

	if ok {
		ts.mu.Lock()
		for _, s := range ts.subs {
			select {
			case s.ch <- ev:
			default:
				// Drop if subscriber is slow — never block the writer.
			}
		}
		ts.mu.Unlock()
	}

	return len(p), nil
}
