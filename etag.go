package main

import (
	"errors"
	"fmt"
	"strings"
)

// ErrPrecondition is a failed If-Match or If-None-Match. 412.
var ErrPrecondition = errors.New("precondition failed")

// etagEntry is one entity-tag from an If-Match or If-None-Match list.
// net/http's parser for these is unexported, so this is the ~20 lines of it
// we need.
type etagEntry struct {
	value string
	weak  bool
}

// TagList is a parsed If-Match or If-None-Match header value.
type TagList struct {
	Any  bool // the header was "*"
	Tags []etagEntry
}

// parseTagList parses an If-Match / If-None-Match value per RFC 9110 §8.8.3.
// ok is false for a malformed value, which the handler turns into 400 rather
// than silently ignoring a precondition the client asked for.
func parseTagList(v string) (*TagList, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, false
	}
	if v == "*" {
		return &TagList{Any: true}, true
	}

	list := &TagList{}
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		weak := false
		if rest, found := strings.CutPrefix(part, `W/`); found {
			weak, part = true, rest
		}
		if len(part) < 2 || part[0] != '"' || part[len(part)-1] != '"' {
			return nil, false
		}
		value := part[1 : len(part)-1]
		if strings.Contains(value, `"`) {
			return nil, false
		}
		list.Tags = append(list.Tags, etagEntry{value: value, weak: weak})
	}
	return list, true
}

// Precondition is the write-path half of RFC 9110 §13. A nil field means the
// header was absent, so the zero value is last-writer-wins — exactly the
// behaviour a caller that sends no precondition had before.
type Precondition struct {
	IfMatch     *TagList
	IfNoneMatch *TagList
}

// check reports whether a write may proceed against the current state.
//
// Evaluated in RFC 9110 §13.2.2 order: If-Match first, then If-None-Match.
// A missing resource with If-Match present is 412, not 404.
func (p Precondition) check(curHash string, exists bool) error {
	if p.IfMatch != nil {
		if !exists || !p.IfMatch.matchStrong(curHash) {
			return fmt.Errorf("%w: If-Match", ErrPrecondition)
		}
	}
	if p.IfNoneMatch != nil {
		if exists && p.IfNoneMatch.matchWeak(curHash) {
			return fmt.Errorf("%w: If-None-Match", ErrPrecondition)
		}
	}
	return nil
}

// matchStrong is If-Match's comparison. RFC 9110 requires strong comparison
// here, so a weak tag never matches — which is fine, because we only ever
// emit strong tags and only strong tags are usable for the lost-update case
// this exists to serve.
func (l *TagList) matchStrong(hash string) bool {
	if l.Any {
		return true
	}
	for _, e := range l.Tags {
		if !e.weak && e.value == hash {
			return true
		}
	}
	return false
}

// matchWeak is If-None-Match's comparison: the weak flag is ignored.
func (l *TagList) matchWeak(hash string) bool {
	if l.Any {
		return true
	}
	for _, e := range l.Tags {
		if e.value == hash {
			return true
		}
	}
	return false
}
