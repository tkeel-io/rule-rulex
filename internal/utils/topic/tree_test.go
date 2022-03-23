package topic

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/stream"
	"testing"

	"github.com/stretchr/testify/assert"
)

type A int

func (a A) String() string {
	panic("implement me")
}

func (a A) Invoke(ctx context.Context, msg stream.PublishMessage) error {
	panic("implement me")
}

func (a A) ID() string { return fmt.Sprintf("%d", a) }

type AB struct {
	a int
	b int
}

func (a AB) ID() string { return fmt.Sprintf("%d", a.a) }

func (a AB) String() string {
	return fmt.Sprintf("%d:%d", a.a, a.b)
}

func (a AB) Invoke(ctx context.Context, msg stream.PublishMessage) error {
	panic("implement me")
}

type ABC struct {
	a   int
	b   string
	c   bool
	ext string
}

func (a *ABC) String() string {
	panic("implement me")
}

func (a *ABC) Invoke(ctx context.Context, msg stream.PublishMessage) error {
	panic("implement me")
}

func (a *ABC) ID() string { return fmt.Sprintf("%d:%s:%t", a.a, a.b, a.c) }

func TestTreeAdd(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.root.children["foo"].children["bar"].values[0])
}

func TestTreeAddDuplicate(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Add("foo/bar", A(1))

	assert.Equal(t, 1, len(tree.root.children["foo"].children["bar"].values))
}

func NewTree() *Tree {
	//trie := NewTrie()
	//tree := trie.Tree("aaaa")
	return &Tree{
		Separator:    "/",
		WildcardOne:  "+",
		WildcardSome: "#",
		root: newNode(),
	}
}

func TestTreeSet(t *testing.T) {
	tree := NewTree()

	tree.Set("foo/bar", A(1))

	assert.Equal(t, A(1), tree.root.children["foo"].children["bar"].values[0])
}

func TestTreeSetReplace(t *testing.T) {
	tree := NewTree()

	tree.Set("foo/bar", A(1))
	tree.Set("foo/bar", A(2))

	assert.Equal(t, A(2), tree.root.children["foo"].children["bar"].values[0])
}

func TestTreeGet(t *testing.T) {
	tree := NewTree()

	tree.Set("foo/#", A(1))

	assert.Equal(t, A(1), tree.Get("foo/#")[0])
}

func TestTreeRemove(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Remove("foo/bar", A(1))
	assert.Equal(t, 0, len(tree.root.children))

	tree.Add("foo/bar", &ABC{})
	tree.Remove("foo/bar", &ABC{})
	assert.Equal(t, 0, len(tree.root.children))

	tree.Add("foo/bar", &ABC{a: 1, b: "abc", c: false})
	tree.Remove("foo/bar", &ABC{a: 1, b: "abc", c: false})
	assert.Equal(t, 0, len(tree.root.children))

	tree.Add("foo/bar", &ABC{a: 1, b: "abc", c: false, ext: "1"})
	tree.Remove("foo/bar", &ABC{a: 1, b: "abc", c: false, ext: "2"})
	assert.Equal(t, 0, len(tree.root.children))

	tree.Add("foo/bar", &ABC{a: 1, b: "bbb", c: false, ext: "1"})
	tree.Remove("foo/bar", &ABC{a: 1, b: "ccc", c: false, ext: "1"})
	assert.Equal(t, 1, len(tree.root.children))
	tree.Remove("foo/bar", &ABC{a: 1, b: "bbb", c: false, ext: "2"})
	assert.Equal(t, 0, len(tree.root.children))

	tree.Add("foo/bar", &ABC{a: 1, b: "bbb", c: false, ext: "1"})
	tree.Add("foo/bar", &ABC{a: 1, b: "ccc", c: false, ext: "1"})
	for i, n := range tree.Match("foo/bar") {
		fmt.Println(i, n)
	}
	tree.Empty("foo/bar")
	assert.Equal(t, 0, len(tree.root.children))

}

func TestTreeRemoveMissing(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Remove("bar/baz", A(1))

	assert.Equal(t, 1, len(tree.root.children))
}

func TestTreeEmpty(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Add("foo/bar", A(2))
	tree.Empty("foo/bar")

	assert.Equal(t, 0, len(tree.root.children))
}

func TestTreeClear(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Add("foo/bar/baz", A(1))
	tree.Clear(A(1))

	assert.Equal(t, 0, len(tree.root.children))
}

func TestTreeMatchExact(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.Match("foo/bar")[0])
}

func TestTreeMatchWildcard1(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/+", A(1))

	assert.Equal(t, A(1), tree.Match("foo/bar")[0])
}

func TestTreeMatchWildcard2(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/#", A(1))

	assert.Equal(t, A(1), tree.Match("foo/bar")[0])
}

func TestTreeMatchWildcard3(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/#", A(1))

	assert.Equal(t, A(1), tree.Match("foo/bar/baz")[0])
}

func TestTreeMatchWildcard4(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar/#", A(1))

	assert.Equal(t, A(1), tree.Match("foo/bar")[0])
}

func TestTreeMatchWildcard5(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/#", A(1))

	assert.Equal(t, A(1), tree.Match("foo/bar/#")[0])
}

func TestTreeMatchMultiple(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Add("foo/+", A(2))
	tree.Add("foo/#", A(3))

	assert.Equal(t, 3, len(tree.Match("foo/bar")))
}

func TestTreeMatchNoDuplicates(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Add("foo/+", A(1))
	tree.Add("foo/#", A(1))

	assert.Equal(t, 1, len(tree.Match("foo/bar")))
}

func TestTreeMatchFirst(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/+", A(1))

	assert.Equal(t, A(1), tree.MatchFirst("foo/bar"))
}

func TestTreeMatchFirstNone(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/+", A(1))

	assert.Nil(t, tree.MatchFirst("baz/qux"))
}

func TestTreeSearchExact(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.Search("foo/bar")[0])
}

func TestTreeSearchWildcard1(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.Search("foo/+")[0])
}

func TestTreeSearchWildcard2(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.Search("foo/#")[0])
}

func TestTreeSearchWildcard3(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar/baz", A(1))

	assert.Equal(t, A(1), tree.Search("foo/#")[0])
}

func TestTreeSearchWildcard4(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.Search("foo/bar/#")[0])
}

func TestTreeSearchWildcard5(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar/#", A(1))

	assert.Equal(t, A(1), tree.Search("foo/#")[0])
}

func TestTreeSearchMultiple(t *testing.T) {
	tree := NewTree()

	tree.Add("foo", A(1))
	tree.Add("foo/bar", A(2))
	tree.Add("foo/bar/baz", A(3))

	assert.Equal(t, 3, len(tree.Search("foo/#")))
}

func TestTreeSearchNoDuplicates(t *testing.T) {
	tree := NewTree()

	tree.Add("foo", A(1))
	tree.Add("foo/bar", A(1))
	tree.Add("foo/bar/baz", A(1))

	assert.Equal(t, 1, len(tree.Search("foo/#")))
}

func TestTreeSearchFirst(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, A(1), tree.SearchFirst("foo/+"))
}

func TestTreeSearchFirstNone(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Nil(t, tree.SearchFirst("baz/qux"))
}

func TestTreeCount(t *testing.T) {
	tree := NewTree()

	tree.Add("foo", A(1))
	tree.Add("foo/bar", A(2))
	tree.Add("foo/bar/baz", A(3))
	tree.Add("foo/bar/baz", A(4))
	tree.Add("quz/bar/baz", A(4))

	assert.Equal(t, 5, tree.Count())
}

func TestTreeAll(t *testing.T) {
	tree := NewTree()

	tree.Add("foo", A(1))
	tree.Add("foo/bar", A(2))
	tree.Add("foo/bar/baz", A(3))

	assert.Equal(t, 3, len(tree.All()))
}

func TestTreeAllNoDuplicates(t *testing.T) {
	tree := NewTree()

	tree.Add("foo", A(1))
	tree.Add("foo/bar", A(1))
	tree.Add("foo/bar/baz", A(1))

	assert.Equal(t, 1, len(tree.All()))
}

func TestTreeReset(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))
	tree.Reset()

	assert.Equal(t, 0, len(tree.root.children))
}

func TestTreeString(t *testing.T) {
	tree := NewTree()

	tree.Add("foo/bar", A(1))

	assert.Equal(t, "topic.Tree:\n| 'foo' => 0\n|   'bar' => 1", tree.String())
}

func BenchmarkTreeAddSame(b *testing.B) {
	tree := NewTree()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Add("foo/bar", A(1))
	}
}

func BenchmarkTreeAddUnique(b *testing.B) {
	tree := NewTree()

	strings := make([]string, 0, b.N)

	for i := 0; i < b.N; i++ {
		strings = append(strings, fmt.Sprintf("foo/%d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Add(strings[i], A(1))
	}
}

func BenchmarkTreeSetSame(b *testing.B) {
	tree := NewTree()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Set("foo/bar", A(1))
	}
}

func BenchmarkTreeSetUnique(b *testing.B) {
	tree := NewTree()

	strings := make([]string, 0, b.N)

	for i := 0; i < b.N; i++ {
		strings = append(strings, fmt.Sprintf("foo/%d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Set(strings[i], A(1))
	}
}

func BenchmarkTreeMatchExact(b *testing.B) {
	tree := NewTree()
	tree.Add("foo/bar", A(1))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Match("foo/bar")
	}
}

func BenchmarkTreeMatchWildcardOne(b *testing.B) {
	tree := NewTree()
	tree.Add("foo/+", A(1))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Match("foo/bar")
	}
}

func BenchmarkTreeMatchWildcardSome(b *testing.B) {
	tree := NewTree()
	tree.Add("#", A(1))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Match("foo/bar")
	}
}

func BenchmarkTreeSearchExact(b *testing.B) {
	tree := NewTree()
	tree.Add("foo/bar", A(1))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Search("foo/bar")
	}
}

func BenchmarkTreeSearchWildcardOne(b *testing.B) {
	tree := NewTree()
	tree.Add("foo/bar", A(1))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Search("foo/+")
	}
}

func BenchmarkTreeSearchWildcardSome(b *testing.B) {
	tree := NewTree()
	tree.Add("foo/bar", A(1))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Search("#")
	}
}

func BenchmarkTreeSearchDeepTree(b *testing.B) {

	tree := NewTree()

	tree.Add("foo/foo/foo/foo/foo/bar", A(1))
	for i := 0; i < 100000; i++ {
		tree.Add(fmt.Sprintf("%d/%d/%d/%d/%d/bar", i/10000, i/1000, i/100, i/10, i), A(1))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tree.Search("foo/foo/foo/foo/foo/+")
	}
}

func ExampleReplaceTree() {

	tree := NewTree()

	tree.Add("foo/foo/foo/foo/foo/bar", AB{1, 1})
	tree.Add("foo/foo/foo/foo/foo/bar", AB{1, 2})
	for idx := 0; idx < 100000; idx++ {
		id := idx % 100
		topic := fmt.Sprintf("%d/bar", id)
		tree.Add(topic, AB{id, idx})
		for _, n := range tree.Match(topic) {
			if n.ID() != fmt.Sprintf("%d", id) ||
				n.String() != fmt.Sprintf("%d:%d", id, idx) {
				fmt.Printf("Error:%s != %s, %s != %s\n",
					n.ID(), fmt.Sprintf("%d", id),
					n.String(), fmt.Sprintf("%d:%d", id, idx))
				break
			}
		}
	}

	// Output:
	//
}
