/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logs

import (
	"bufio"
	"fmt"
	"io"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// circularLinkedList is an append-only linked list, and behaves like a ring buffer.
type circularLinkedList struct {
	capacity, length int

	// head always points to the first node of the list.
	head *circularLinkedListNode
	// current points to the last added or modified node.
	current *circularLinkedListNode
}

type circularLinkedListNode struct {
	value indexItem
	next  *circularLinkedListNode
}

func newCircularLinkedList(cap int) (*circularLinkedList, error) {
	if cap < 0 {
		return nil, fmt.Errorf("invalid capacity: %d", cap)
	}
	return &circularLinkedList{
		capacity: cap,
	}, nil
}

func (l *circularLinkedList) Add(val indexItem) {
	if l.capacity == 0 {
		return
	}

	if l.length == 0 {
		node := &circularLinkedListNode{
			value: val,
		}
		node.next = node
		l.head = node
		l.current = node
		l.length = 1
		return
	}

	if l.length == l.capacity {
		// The linked list is full, so we overwrite the first node.
		l.current = l.current.next
		l.current.value = val
		return
	}

	// Append a new node, and points the current pointer to the new node.
	// The `next` pointer of the new node points to the `head` to keep the list circular.
	l.length++
	l.current.next = &circularLinkedListNode{
		value: val,
		next:  l.head,
	}
	l.current = l.current.next
}

func (l *circularLinkedList) Len() int {
	return l.length
}

func (l *circularLinkedList) VisitAll(f func(indexItem) error) error {
	if l.length == 0 {
		// the list is empty
		return nil
	}
	for p, i := l.current.next, 0; i < l.length; p, i = p.next, i+1 {
		if err := f(p.value); err != nil {
			return err
		}
	}
	return nil
}

type indexItem struct {
	offset int64
	length int64
}

type logFilterResult struct {
	logIndex       *circularLinkedList
	maxLogLength   int64
	processedBytes int64
}

func filterLogByStream(f io.ReadSeeker, tailLines int64, wantStream runtimeapi.LogStreamType) (*logFilterResult, error) {
	curSize, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to end of log file: %w", err)
	}
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek to front of log file: %w", err)
	}

	br := bufio.NewReader(f)
	l, err := newCircularLinkedList(int(tailLines))
	if err != nil {
		return nil, err
	}

	var readBytes, processedBytes, maxLength int64
	for {
		line, err := br.ReadBytes(eol[0])
		lineLength := int64(len(line))
		readBytes += lineLength
		if err == io.EOF || readBytes > curSize {
			// the line is incomplete, so we should skip it
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read line: %w", err)
		}

		streamType, err := extractStreamFromLog(line)
		if err != nil {
			return nil, err
		}
		if streamType == wantStream {
			if lineLength > maxLength {
				maxLength = lineLength
			}
			// We save the offset and length of the line rather than the entire line, in case the line is very long,
			// which might lead to OOM.
			l.Add(indexItem{
				offset: processedBytes,
				length: lineLength,
			})
		}
		processedBytes += int64(len(line))
	}

	return &logFilterResult{
		logIndex:       l,
		maxLogLength:   maxLength,
		processedBytes: processedBytes,
	}, nil
}
