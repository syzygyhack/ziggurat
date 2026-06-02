package cluster

import (
	"strconv"
	"testing"
)

// Regression: the periodic capability refresh keeps a caps map and mutates it
// in place each tick while handing it to the delegate, which memberlist
// marshals concurrently via NodeMeta(). The delegate must deep-copy so it
// never shares a mutable map with the caller. Run with -race.
func TestDelegate_NoRaceOnConcurrentRefreshAndGossip(t *testing.T) {
	log := testLogger()
	caps := map[string]string{"os": "linux", "cpu.cores": "8", "disk.avail": "0"}
	d, err := newDelegate(&NodeMeta{ID: "n1", Role: "hybrid", Caps: caps}, log)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			caps["disk.avail"] = strconv.Itoa(i) // caller mutates ITS map (refresh tick)
			d.UpdateMeta(&NodeMeta{ID: "n1", Role: "hybrid", Caps: caps})
		}
		close(done)
	}()
	for {
		select {
		case <-done:
			return
		default:
			_ = d.NodeMeta(512) // memberlist marshals the delegate's meta
		}
	}
}
