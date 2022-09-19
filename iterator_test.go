package gosqliterator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type queue[T any] struct {
	elements []T
	err      error
	count    int
}

func (q queue[T]) slice() ([]T, error) {
	return remove(q.elements, q.count), q.err
}

var queries []string
var values [][]string

func TestChunk(t *testing.T) {
	queue := queue[string]{
		elements: []string{"a", "b", "c", "d", "x", "y", "z", "w"},
		count:    2,
	}

	iterator := Iterate(func(query string, args ...interface{}) ([]string, error) {
		queries = append(queries, query)
		return queue.slice()
	}, Q("SELECT * FROM events").WithLimit(2))

	assert.True(t, iterator.Valid())

	elements1, err := iterator.Next()
	values = append(values, elements1)
	assert.True(t, iterator.Valid())
	assert.Nil(t, err)

	elements2, err := iterator.Next()
	values = append(values, elements2)
	assert.True(t, iterator.Valid())
	assert.Nil(t, err)

	elements3, err := iterator.Next()
	values = append(values, elements3)
	assert.True(t, iterator.Valid())
	assert.Nil(t, err)

	elements4, err := iterator.Next()
	values = append(values, elements4)
	assert.True(t, iterator.Valid())
	assert.Nil(t, err)

	assert.Equal(t, 4, len(queries))
	assert.Equal(t, "SELECT * FROM events LIMIT 2 OFFSET 0", queries[0])
	assert.Equal(t, "SELECT * FROM events LIMIT 2 OFFSET 2", queries[1])
	assert.Equal(t, "SELECT * FROM events LIMIT 2 OFFSET 4", queries[2])
	assert.Equal(t, "SELECT * FROM events LIMIT 2 OFFSET 6", queries[3])
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}
