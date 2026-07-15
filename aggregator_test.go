package wsaggregator

import (
	"context"
	"testing"
	"time"

	pb "go.viam.com/api/service/worldstatestore/v1"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/services/worldstatestore"
	"go.viam.com/test"
)

func newAggregatorForTest() *aggregator {
	return newAggregatorForTestWithBuffer(defaultSubscriberBufferSize)
}

func newAggregatorForTestWithBuffer(buf int) *aggregator {
	return &aggregator{
		logger:      logging.NewTestLogger(&testing.T{}),
		store:       newStore(),
		subscribers: make(map[uint64]*subscriber),
		bufferSize:  buf,
	}
}

func recvWithin(t *testing.T, ch <-chan worldstatestore.TransformChange, d time.Duration) worldstatestore.TransformChange {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(d):
		t.Fatal("timeout waiting for change event")
		return worldstatestore.TransformChange{}
	}
}

func expectNoRecv(t *testing.T, ch <-chan worldstatestore.TransformChange, d time.Duration) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("unexpected change event: %+v", ev)
		}
	case <-time.After(d):
	}
}

func setTransform(t *testing.T, a *aggregator, uuid, refFrame string) {
	t.Helper()
	_, err := a.DoCommand(context.Background(), map[string]interface{}{
		"set_transform": map[string]interface{}{
			"uuid":            uuid,
			"reference_frame": refFrame,
		},
	})
	test.That(t, err, test.ShouldBeNil)
}

func TestEmptyStore(t *testing.T) {
	a := newAggregatorForTest()

	uuids, err := a.ListUUIDs(context.Background(), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, uuids, test.ShouldBeEmpty)

	_, err = a.GetTransform(context.Background(), []byte("missing"), nil)
	test.That(t, err, test.ShouldEqual, worldstatestore.ErrNilResponse)
}

func TestSetGetRoundTrip(t *testing.T) {
	a := newAggregatorForTest()
	setTransform(t, a, "tool/attached", "gripper-1")

	got, err := a.GetTransform(context.Background(), []byte("tool/attached"), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got, test.ShouldNotBeNil)
	test.That(t, got.ReferenceFrame, test.ShouldEqual, "gripper-1")
	test.That(t, string(got.Uuid), test.ShouldEqual, "tool/attached")

	uuids, err := a.ListUUIDs(context.Background(), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, uuids, test.ShouldHaveLength, 1)
	test.That(t, string(uuids[0]), test.ShouldEqual, "tool/attached")
}

func TestSetUpsertsExisting(t *testing.T) {
	a := newAggregatorForTest()
	setTransform(t, a, "u1", "frame-a")
	setTransform(t, a, "u1", "frame-b")

	got, err := a.GetTransform(context.Background(), []byte("u1"), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, got.ReferenceFrame, test.ShouldEqual, "frame-b")

	uuids, err := a.ListUUIDs(context.Background(), nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, uuids, test.ShouldHaveLength, 1)
}

func TestRemoveThenGetReturnsNil(t *testing.T) {
	a := newAggregatorForTest()
	setTransform(t, a, "u1", "f")

	_, err := a.DoCommand(context.Background(), map[string]interface{}{
		"remove_transform": map[string]interface{}{"uuid": "u1"},
	})
	test.That(t, err, test.ShouldBeNil)

	_, err = a.GetTransform(context.Background(), []byte("u1"), nil)
	test.That(t, err, test.ShouldEqual, worldstatestore.ErrNilResponse)
}

func TestRemoveUnknownIsNoop(t *testing.T) {
	a := newAggregatorForTest()
	_, err := a.DoCommand(context.Background(), map[string]interface{}{
		"remove_transform": map[string]interface{}{"uuid": "never-set"},
	})
	test.That(t, err, test.ShouldBeNil)
}

func TestListTransformsReturnsAll(t *testing.T) {
	a := newAggregatorForTest()
	setTransform(t, a, "u1", "f1")
	setTransform(t, a, "u2", "f2")

	res, err := a.DoCommand(context.Background(), map[string]interface{}{
		"list_transforms": map[string]interface{}{},
	})
	test.That(t, err, test.ShouldBeNil)
	list, ok := res["transforms"].([]map[string]interface{})
	test.That(t, ok, test.ShouldBeTrue)
	test.That(t, list, test.ShouldHaveLength, 2)
}

func TestUnknownCommandErrors(t *testing.T) {
	a := newAggregatorForTest()
	_, err := a.DoCommand(context.Background(), map[string]interface{}{"nope": true})
	test.That(t, err, test.ShouldNotBeNil)
	test.That(t, err.Error(), test.ShouldContainSubstring, "unknown command")
}

func TestSetTransformValidation(t *testing.T) {
	a := newAggregatorForTest()

	cases := []struct {
		name    string
		payload interface{}
		wantMsg string
	}{
		{"non-object", "not-an-object", "must be an object"},
		{"missing uuid", map[string]interface{}{"reference_frame": "f"}, "missing required field"},
		{"non-string uuid", map[string]interface{}{"uuid": 42}, "must be a string"},
		{"empty uuid", map[string]interface{}{"uuid": ""}, "non-empty"},
		{"bad transform fields", map[string]interface{}{"uuid": "u", "reference_frame": 42}, "invalid transform fields"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.DoCommand(context.Background(), map[string]interface{}{"set_transform": tc.payload})
			test.That(t, err, test.ShouldNotBeNil)
			test.That(t, err.Error(), test.ShouldContainSubstring, tc.wantMsg)
		})
	}
}

func TestRemoveTransformValidation(t *testing.T) {
	a := newAggregatorForTest()

	cases := []struct {
		name    string
		payload interface{}
		wantMsg string
	}{
		{"non-object", 42, "must be an object"},
		{"missing uuid", map[string]interface{}{}, "missing required field"},
		{"empty uuid", map[string]interface{}{"uuid": ""}, "non-empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.DoCommand(context.Background(), map[string]interface{}{"remove_transform": tc.payload})
			test.That(t, err, test.ShouldNotBeNil)
			test.That(t, err.Error(), test.ShouldContainSubstring, tc.wantMsg)
		})
	}
}

func TestNoReplayOnSubscribe(t *testing.T) {
	a := newAggregatorForTest()
	setTransform(t, a, "existing", "f")

	sub := a.register()
	defer a.unregister(sub)

	expectNoRecv(t, sub.ch, 50*time.Millisecond)
}

func TestSubscriberReceivesAddedUpdatedRemoved(t *testing.T) {
	a := newAggregatorForTest()
	sub := a.register()
	defer a.unregister(sub)

	setTransform(t, a, "u1", "f-a")
	added := recvWithin(t, sub.ch, 200*time.Millisecond)
	test.That(t, added.ChangeType, test.ShouldEqual, pb.TransformChangeType_TRANSFORM_CHANGE_TYPE_ADDED)
	test.That(t, added.Transform.ReferenceFrame, test.ShouldEqual, "f-a")
	test.That(t, string(added.Transform.Uuid), test.ShouldEqual, "u1")

	setTransform(t, a, "u1", "f-b")
	updated := recvWithin(t, sub.ch, 200*time.Millisecond)
	test.That(t, updated.ChangeType, test.ShouldEqual, pb.TransformChangeType_TRANSFORM_CHANGE_TYPE_UPDATED)
	test.That(t, updated.Transform.ReferenceFrame, test.ShouldEqual, "f-b")

	_, err := a.DoCommand(context.Background(), map[string]interface{}{
		"remove_transform": map[string]interface{}{"uuid": "u1"},
	})
	test.That(t, err, test.ShouldBeNil)
	removed := recvWithin(t, sub.ch, 200*time.Millisecond)
	test.That(t, removed.ChangeType, test.ShouldEqual, pb.TransformChangeType_TRANSFORM_CHANGE_TYPE_REMOVED)
	test.That(t, string(removed.Transform.Uuid), test.ShouldEqual, "u1")
}

func TestRemoveUnknownDoesNotPublish(t *testing.T) {
	a := newAggregatorForTest()
	sub := a.register()
	defer a.unregister(sub)

	_, err := a.DoCommand(context.Background(), map[string]interface{}{
		"remove_transform": map[string]interface{}{"uuid": "nope"},
	})
	test.That(t, err, test.ShouldBeNil)
	expectNoRecv(t, sub.ch, 50*time.Millisecond)
}

func TestMultipleSubscribersEachReceive(t *testing.T) {
	a := newAggregatorForTest()
	s1 := a.register()
	defer a.unregister(s1)
	s2 := a.register()
	defer a.unregister(s2)

	setTransform(t, a, "u1", "f")
	e1 := recvWithin(t, s1.ch, 200*time.Millisecond)
	e2 := recvWithin(t, s2.ch, 200*time.Millisecond)
	test.That(t, e1.ChangeType, test.ShouldEqual, pb.TransformChangeType_TRANSFORM_CHANGE_TYPE_ADDED)
	test.That(t, e2.ChangeType, test.ShouldEqual, pb.TransformChangeType_TRANSFORM_CHANGE_TYPE_ADDED)
}

func TestSlowSubscriberDoesNotBlockPublisher(t *testing.T) {
	const buf = 2
	const N = 50
	a := newAggregatorForTestWithBuffer(buf)
	slow := a.register()
	defer a.unregister(slow)

	// slow.ch is never drained. If publish were blocking, this loop would hang
	// once the buffer fills; reaching the assertions proves publish is
	// non-blocking under a full subscriber.
	for i := 0; i < N; i++ {
		setTransform(t, a, "u1", "f")
	}
	test.That(t, len(slow.ch), test.ShouldEqual, buf)
}

func TestCtxCancelUnregisters(t *testing.T) {
	a := newAggregatorForTest()
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := a.StreamTransformChanges(ctx, nil)
	test.That(t, err, test.ShouldBeNil)
	test.That(t, stream, test.ShouldNotBeNil)

	// One subscriber registered.
	a.subMu.Lock()
	count := len(a.subscribers)
	a.subMu.Unlock()
	test.That(t, count, test.ShouldEqual, 1)

	cancel()

	// Give the ctx-watcher goroutine a moment to unregister.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		a.subMu.Lock()
		count = len(a.subscribers)
		a.subMu.Unlock()
		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	test.That(t, count, test.ShouldEqual, 0)
}
