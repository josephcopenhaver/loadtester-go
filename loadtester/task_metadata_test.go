package loadtester

import (
	"context"
	"testing"
	"time"
)

func TestMetadataProviderInterfaceStability(t *testing.T) {
	type taskMetadataProvider interface {
		IntervalID() time.Time
		EnqueueTime() time.Time
		DequeueTime() time.Time
		SampleSize() int
		NumIntervalTasks() int
		Lag() time.Duration
	}

	now := time.Now()
	v := taskMetadataProvider(&taskMetadata{taskMetadataState{
		meta: taskMeta{
			IntervalID:       now,
			SampleSize:       2,
			NumIntervalTasks: 3,
			Lag:              2 * time.Second,
		},
		enqueueTime: now.Add(1 * time.Minute),
		dequeueTime: now.Add(2 * time.Minute),
	}})

	if !v.IntervalID().Equal(now) {
		t.Fatal()
	}

	if !v.EnqueueTime().Equal(now.Add(1 * time.Minute)) {
		t.Fatal()
	}

	if !v.DequeueTime().Equal(now.Add(2 * time.Minute)) {
		t.Fatal()
	}

	if v.SampleSize() != 2 {
		t.Fatal()
	}

	if v.NumIntervalTasks() != 3 {
		t.Fatal()
	}

	if v.Lag() != 2*time.Second {
		t.Fatal()
	}
}

func TestMetadataProviderNilable(t *testing.T) {
	if v := GetTaskMetadataProvider(context.Background()); v != nil {
		t.Fatal()
	}
}
