package gosqliterator

import (
	"errors"
	"fmt"
)

const (
	defaultLimit uint = 10
)

var (
	ErrEmptyIterator = errors.New("iterator empty")
)

// Fetcher fetch the data from any provided SQL storage.
type Fetcher[T any] func(query string, args ...interface{}) ([]T, error)

// Query contains the metadata for executing the query.
type Query struct {
	// SELECT * FROM events, SELECT * FROM events ORDER BY col...
	sql string
	// how many rows should we take in one query
	limit uint
	// slice of args where will be pass to the query
	args []interface{}
}

// QueryIterator represents iterator for query generic values from Fetcher.
type QueryIterator[T any] struct {
	query   Query
	fetcher Fetcher[T]
	valid   bool
	page    int
}

func (iter *QueryIterator[T]) Next() (elements []T, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("can't iterate %w", err)
		}
	}()

	if !iter.valid {
		return nil, ErrEmptyIterator
	}

	elements, err = iter.fetcher(iter.sql(), iter.query.args...)
	if err != nil {
		return
	}

	elementsLength := len(elements)
	if elementsLength == 0 {
		iter.valid = false
		return
	}

	iter.page += 1
	return
}

func (iter *QueryIterator[T]) Valid() bool {
	return iter.valid
}

func (iter *QueryIterator[T]) sql() string {
	return iter.query.sql + fmt.Sprintf(" LIMIT %d OFFSET %d", iter.query.limit, iter.offset())
}

func (iter *QueryIterator[T]) offset() int {
	return (iter.page - 1) * int(iter.query.limit)
}

// Iterate creates the QueryIterator.
func Iterate[T any](f Fetcher[T], q Query) *QueryIterator[T] {
	return &QueryIterator[T]{
		fetcher: f,
		query:   q,
		valid:   true,
		page:    1,
	}
}

func Q(sql string) Query {
	return Query{
		sql:   sql,
		limit: defaultLimit,
		args:  make([]interface{}, 0),
	}
}

func (q Query) WithLimit(limit uint) Query {
	q.limit = limit
	return q
}

func (q Query) WithArgs(args ...interface{}) Query {
	q.args = append(q.args, args...)
	return q
}
