package sqlcommon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// recordingRows is a streaming Rows source that records how it was consumed.
type recordingRows struct {
	columns []string
	rows    [][]any
	pos     int
	err     error
	closed  bool
}

func newRecordingRows(rows ...[]any) *recordingRows {
	return &recordingRows{columns: slices.Clone(SQLIteratorColumns()), rows: rows, pos: -1}
}

func (r *recordingRows) Columns() ([]string, error) { return r.columns, nil }
func (r *recordingRows) Err() error                 { return r.err }

func (r *recordingRows) Close() error {
	r.closed = true
	return nil
}

func (r *recordingRows) Next() bool {
	if r.closed {
		panic("recordingRows: Next after Close")
	}
	r.pos++
	return r.pos < len(r.rows)
}

func (r *recordingRows) Scan(dest ...any) error {
	row := r.rows[r.pos]
	if len(dest) != len(row) {
		return fmt.Errorf("recordingRows: %d scan targets for %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(row[i]))
	}
	return nil
}

var tupleRowInsertedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func tupleRow(object, user, ulid string) []any {
	objectType, objectID := tupleUtils.SplitObject(object)
	return []any{"store", objectType, objectID, "viewer", user, sql.NullString{}, []byte(nil), ulid, tupleRowInsertedAt}
}

func TestBufferRows(t *testing.T) {
	t.Run("drains_and_closes_the_source", func(t *testing.T) {
		src := newRecordingRows(
			tupleRow("doc:1", "user:anne", "01A"),
			tupleRow("doc:2", "user:bob", "01B"),
		)
		buffered, err := bufferRows(src)
		require.NoError(t, err)
		require.True(t, src.closed)
		require.Equal(t, len(src.rows), src.pos)

		var got []string
		for buffered.Next() {
			var store, objectType, objectID, relation, user, ulid string
			var conditionName sql.NullString
			var conditionContext []byte
			var insertedAt time.Time
			require.NoError(t, buffered.Scan(&store, &objectType, &objectID, &relation, &user,
				&conditionName, &conditionContext, &ulid, &insertedAt))
			require.Equal(t, tupleRowInsertedAt, insertedAt)
			got = append(got, fmt.Sprintf("%s:%s#%s@%s/%s", objectType, objectID, relation, user, ulid))
		}
		require.NoError(t, buffered.Err())
		require.Equal(t, []string{"doc:1#viewer@user:anne/01A", "doc:2#viewer@user:bob/01B"}, got)
		require.False(t, buffered.Next())
	})

	t.Run("rejects_unexpected_columns_before_reading_a_row", func(t *testing.T) {
		for name, columns := range map[string][]string{
			"missing": SQLIteratorColumns()[:8],
			"extra":   append(slices.Clone(SQLIteratorColumns()), "user_type"),
			"reordered": func() []string {
				c := slices.Clone(SQLIteratorColumns())
				c[1], c[2] = c[2], c[1]
				return c
			}(),
		} {
			t.Run(name, func(t *testing.T) {
				src := newRecordingRows()
				src.columns = columns
				_, err := bufferRows(src)
				require.ErrorContains(t, err, "result columns")
				require.Equal(t, -1, src.pos)
				require.True(t, src.closed)
			})
		}
	})

	t.Run("propagates_the_source_error", func(t *testing.T) {
		boom := errors.New("connection reset")
		src := newRecordingRows(tupleRow("doc:1", "user:anne", "01A"))
		src.err = boom
		_, err := bufferRows(src)
		require.ErrorIs(t, err, boom)
		require.True(t, src.closed)
	})

	t.Run("propagates_scan_errors", func(t *testing.T) {
		src := newRecordingRows(tupleRow("doc:1", "user:anne", "01A")[:8])
		_, err := bufferRows(src)
		require.ErrorContains(t, err, "scan targets")
		require.True(t, src.closed)
	})
}

func TestBufferedRowsScan(t *testing.T) {
	newBuffered := func(t *testing.T) *bufferedRows {
		buffered, err := bufferRows(newRecordingRows(tupleRow("doc:1", "user:anne", "01A")))
		require.NoError(t, err)
		return buffered
	}
	targets := func() []any {
		var store, objectType, objectID, relation, user, ulid string
		var conditionName sql.NullString
		var conditionContext []byte
		var insertedAt time.Time
		return []any{&store, &objectType, &objectID, &relation, &user, &conditionName, &conditionContext, &ulid, &insertedAt}
	}

	t.Run("rejects_scan_without_a_current_row", func(t *testing.T) {
		buffered := newBuffered(t)
		require.ErrorContains(t, buffered.Scan(targets()...), "Scan at position -1")
		require.True(t, buffered.Next())
		require.NoError(t, buffered.Scan(targets()...))
		require.False(t, buffered.Next())
		require.False(t, buffered.Next())
		require.ErrorContains(t, buffered.Scan(targets()...), "Scan at position 1 of 1")
	})

	t.Run("rejects_a_wrong_target_count", func(t *testing.T) {
		buffered := newBuffered(t)
		require.True(t, buffered.Next())
		require.ErrorContains(t, buffered.Scan(targets()[:8]...), "8 scan targets, want 9")
	})

	t.Run("rejects_a_wrong_target_type", func(t *testing.T) {
		buffered := newBuffered(t)
		require.True(t, buffered.Next())
		dest := targets()
		var insertedAt string
		dest[8] = &insertedAt
		require.ErrorContains(t, buffered.Scan(dest...), "scan target *string, want *time.Time")
	})
}

func TestSQLTupleIteratorReleasesSourceBeforeFirstTuple(t *testing.T) {
	ctx := context.Background()
	rows := [][]any{
		tupleRow("doc:1", "user:anne", "01A"),
		tupleRow("doc:2", "user:anne", "01B"),
		tupleRow("doc:3", "user:anne", "01C"),
	}

	for name, first := range map[string]func(*SQLTupleIterator) (string, error){
		"next": func(iter *SQLTupleIterator) (string, error) {
			tuple, err := iter.Next(ctx)
			return tuple.GetKey().GetObject(), err
		},
		"head": func(iter *SQLTupleIterator) (string, error) {
			tuple, err := iter.Head(ctx)
			return tuple.GetKey().GetObject(), err
		},
	} {
		t.Run(name, func(t *testing.T) {
			src := newRecordingRows(rows...)
			iter := NewSQLTupleIterator(&stubRowGetter{rows: src}, identityErrHandler)
			defer iter.Stop()

			object, err := first(iter)
			require.NoError(t, err)
			require.Equal(t, "doc:1", object)
			require.True(t, src.closed, "source still open after the first tuple")

			var objects []string
			for {
				tuple, err := iter.Next(ctx)
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}
				require.NoError(t, err)
				objects = append(objects, tuple.GetKey().GetObject())
			}
			if name == "head" {
				require.Equal(t, []string{"doc:1", "doc:2", "doc:3"}, objects)
			} else {
				require.Equal(t, []string{"doc:2", "doc:3"}, objects)
			}
		})
	}

	t.Run("column_mismatch_fails_the_first_read", func(t *testing.T) {
		src := newRecordingRows(rows...)
		src.columns = src.columns[:8]
		iter := NewSQLTupleIterator(&stubRowGetter{rows: src}, identityErrHandler)
		defer iter.Stop()

		_, err := iter.Next(ctx)
		require.ErrorContains(t, err, "result columns")
		require.True(t, src.closed)
	})
}
