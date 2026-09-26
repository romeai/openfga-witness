package sqlcommon

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"
)

// bufferedRows holds a fully drained tuple result set. Draining at query
// time returns the pooled connection before any consumer of the iterator
// dispatches nested reads; a streaming cursor would hold it across them,
// and enough concurrent holders exhaust the pool with every holder waiting
// on a nested read (hold-and-wait).
type bufferedRows struct {
	records []bufferedRecord
	next    int
}

// bufferedRecord has one field per entry of sqlIteratorColumns, in order.
type bufferedRecord struct {
	store, objectType, objectID, relation, user string
	conditionName                               sql.NullString
	conditionContext                            []byte
	ulid                                        string
	insertedAt                                  time.Time
}

// bufferRows drains rows and closes them. The result set must have exactly
// the columns of sqlIteratorColumns; any other shape is rejected before a
// single row is read, including for an empty result.
func bufferRows(rows Rows) (*bufferedRows, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !slices.Equal(columns, sqlIteratorColumns) {
		return nil, fmt.Errorf("bufferRows: result columns %v, want %v", columns, sqlIteratorColumns)
	}
	buffered := &bufferedRows{next: -1}
	for rows.Next() {
		var r bufferedRecord
		if err := rows.Scan(&r.store, &r.objectType, &r.objectID, &r.relation, &r.user,
			&r.conditionName, &r.conditionContext, &r.ulid, &r.insertedAt); err != nil {
			return nil, err
		}
		buffered.records = append(buffered.records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return buffered, nil
}

func (b *bufferedRows) Close() error { return nil }
func (b *bufferedRows) Err() error   { return nil }

func (b *bufferedRows) Next() bool {
	if b.next < len(b.records) {
		b.next++
	}
	return b.next < len(b.records)
}

func (b *bufferedRows) Scan(dest ...any) error {
	if b.next < 0 || b.next >= len(b.records) {
		return fmt.Errorf("bufferedRows: Scan at position %d of %d records", b.next, len(b.records))
	}
	if len(dest) != len(sqlIteratorColumns) {
		return fmt.Errorf("bufferedRows: %d scan targets, want %d", len(dest), len(sqlIteratorColumns))
	}
	r := b.records[b.next]
	return errors.Join(
		assignScanTarget(dest[0], r.store),
		assignScanTarget(dest[1], r.objectType),
		assignScanTarget(dest[2], r.objectID),
		assignScanTarget(dest[3], r.relation),
		assignScanTarget(dest[4], r.user),
		assignScanTarget(dest[5], r.conditionName),
		assignScanTarget(dest[6], r.conditionContext),
		assignScanTarget(dest[7], r.ulid),
		assignScanTarget(dest[8], r.insertedAt),
	)
}

func assignScanTarget[T any](dest any, value T) error {
	target, ok := dest.(*T)
	if !ok {
		return fmt.Errorf("bufferedRows: scan target %T, want %T", dest, target)
	}
	*target = value
	return nil
}
