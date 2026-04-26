package worker

import (
	"testing"
	"time"
)

func TestLogBroadcaster_SubscribeReceivesWrites(t *testing.T) {
	lb := NewLogBroadcaster()
	defer lb.Close("task-1")

	ch, unsub := lb.Subscribe("task-1", 16)
	defer unsub()

	// Write to stdout and stderr.
	w := lb.Writer("task-1", "stdout")
	w.Write([]byte("hello\n"))

	w2 := lb.Writer("task-1", "stderr")
	w2.Write([]byte("error\n"))

	// Should receive both events.
	select {
	case ev := <-ch:
		if ev.Stream != "stdout" || ev.Data != "hello\n" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stdout event")
	}

	select {
	case ev := <-ch:
		if ev.Stream != "stderr" || ev.Data != "error\n" {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stderr event")
	}
}

func TestLogBroadcaster_MultipleSubscribers(t *testing.T) {
	lb := NewLogBroadcaster()
	defer lb.Close("task-1")

	ch1, unsub1 := lb.Subscribe("task-1", 16)
	defer unsub1()

	ch2, unsub2 := lb.Subscribe("task-1", 16)
	defer unsub2()

	w := lb.Writer("task-1", "stdout")
	w.Write([]byte("line1\n"))

	// Both subscribers should get the event.
	for i, ch := range []<-chan LogEvent{ch1, ch2} {
		select {
		case ev := <-ch:
			if ev.Data != "line1\n" {
				t.Fatalf("sub %d: unexpected data: %q", i, ev.Data)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub %d: timed out", i)
		}
	}
}

func TestLogBroadcaster_UnsubscribeStopsDelivery(t *testing.T) {
	lb := NewLogBroadcaster()
	defer lb.Close("task-1")

	ch, unsub := lb.Subscribe("task-1", 16)
	unsub()

	w := lb.Writer("task-1", "stdout")
	w.Write([]byte("after unsub\n"))

	// Channel should be closed or empty — no new events.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed after unsubscribe")
		}
	case <-time.After(100 * time.Millisecond):
		// Also acceptable — no event delivered.
	}
}

func TestLogBroadcaster_CloseSignalsDone(t *testing.T) {
	lb := NewLogBroadcaster()

	ch, _ := lb.Subscribe("task-1", 16)

	w := lb.Writer("task-1", "stdout")
	w.Write([]byte("before close\n"))

	lb.Close("task-1")

	// Drain events — last should be empty (channel closed).
	gotEvent := false
	for ev := range ch {
		if ev.Data == "before close\n" {
			gotEvent = true
		}
	}
	if !gotEvent {
		t.Fatal("expected to receive event before close")
	}
}

func TestLogBroadcaster_WriteToNoSubscribers(t *testing.T) {
	lb := NewLogBroadcaster()
	defer lb.Close("task-1")

	// Writing with no subscribers should not panic or block.
	w := lb.Writer("task-1", "stdout")
	n, err := w.Write([]byte("nobody listening\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len("nobody listening\n") {
		t.Fatalf("expected %d bytes written, got %d", len("nobody listening\n"), n)
	}
}

func TestLogBroadcaster_HasSubscribers(t *testing.T) {
	lb := NewLogBroadcaster()
	defer lb.Close("task-1")

	if lb.HasSubscribers("task-1") {
		t.Fatal("expected no subscribers initially")
	}

	_, unsub := lb.Subscribe("task-1", 16)
	if !lb.HasSubscribers("task-1") {
		t.Fatal("expected subscribers after subscribe")
	}

	unsub()
	if lb.HasSubscribers("task-1") {
		t.Fatal("expected no subscribers after unsubscribe")
	}
}
