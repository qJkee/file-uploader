package etag

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseTagList(t *testing.T) {
	cases := []struct {
		in   string
		want *TagList
	}{
		{`"a"`, &TagList{Tags: []etagEntry{{value: "a"}}}},
		{`"a", "b"`, &TagList{Tags: []etagEntry{{value: "a"}, {value: "b"}}}},
		{`"a","b"`, &TagList{Tags: []etagEntry{{value: "a"}, {value: "b"}}}},
		{`*`, &TagList{Any: true}},
		{` * `, &TagList{Any: true}},
		{`W/"a"`, &TagList{Tags: []etagEntry{{value: "a", weak: true}}}},
		{`W/"a", "b"`, &TagList{Tags: []etagEntry{{value: "a", weak: true}, {value: "b"}}}},
	}
	for _, c := range cases {
		got, ok := ParseList(c.in)
		if !ok {
			t.Errorf("ParseList(%q) = not ok, want ok", c.in)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseList(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}

	malformed := []string{``, `   `, `a`, `"a",`, `"a" "b"`, `"a`, `w/"a"`, `*, "a"`, `"a", *`}
	for _, in := range malformed {
		if _, ok := ParseList(in); ok {
			t.Errorf("ParseList(%q) = ok, want malformed", in)
		}
	}
}

func TestPreconditionCheck(t *testing.T) {
	strong := func(vs ...string) *TagList {
		l := &TagList{}
		for _, v := range vs {
			l.Tags = append(l.Tags, etagEntry{value: v})
		}
		return l
	}
	any := &TagList{Any: true}
	weak := &TagList{Tags: []etagEntry{{value: "h", weak: true}}}

	cases := []struct {
		name    string
		pre     Precondition
		hash    string
		exists  bool
		wantErr bool
	}{
		{"no headers, exists", Precondition{}, "h", true, false},
		{"no headers, absent", Precondition{}, "", false, false},
		{"if-match hit", Precondition{IfMatch: strong("h")}, "h", true, false},
		{"if-match miss", Precondition{IfMatch: strong("x")}, "h", true, true},
		{"if-match on a missing object is 412 not 404", Precondition{IfMatch: strong("h")}, "", false, true},
		{"if-match star, exists", Precondition{IfMatch: any}, "h", true, false},
		{"if-match star, absent", Precondition{IfMatch: any}, "", false, true},
		{"if-match weak tag fails strong comparison", Precondition{IfMatch: weak}, "h", true, true},
		{"if-match list, second matches", Precondition{IfMatch: strong("a", "h")}, "h", true, false},
		{"if-none-match star, exists", Precondition{IfNoneMatch: any}, "h", true, true},
		{"if-none-match star, absent", Precondition{IfNoneMatch: any}, "", false, false},
		{"if-none-match tag hit", Precondition{IfNoneMatch: strong("h")}, "h", true, true},
		{"if-none-match tag miss", Precondition{IfNoneMatch: strong("x")}, "h", true, false},
		{"if-none-match weak tag uses weak comparison", Precondition{IfNoneMatch: weak}, "h", true, true},
		{
			"if-match is evaluated first",
			Precondition{IfMatch: strong("x"), IfNoneMatch: strong("y")},
			"h", true, true,
		},
	}
	for _, c := range cases {
		err := c.pre.Check(c.hash, c.exists)
		if c.wantErr && !errors.Is(err, ErrPrecondition) {
			t.Errorf("%s: check = %v, want ErrPrecondition", c.name, err)
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s: check = %v, want nil", c.name, err)
		}
	}
}
