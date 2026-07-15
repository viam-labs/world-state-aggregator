package wsaggregator

import (
	"context"
	"testing"

	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/services/worldstatestore"
	"go.viam.com/test"
)

func newAggregatorForTest() *aggregator {
	return &aggregator{
		logger: logging.NewTestLogger(&testing.T{}),
		store:  newStore(),
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
