// Copyright 2016 Mike Scherbakov
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package linkedmap

type mapValue struct {
	element *Element
	value   interface{}
}

type Element struct {
	next, prev *Element
	list       *LinkedMap
	key        interface{}
}

type LinkedMap struct {
	Map   map[interface{}]mapValue
	first *Element
	last  *Element
}

func New() *LinkedMap {
	return &LinkedMap{Map: make(map[interface{}]mapValue)}
}

func (lm *LinkedMap) Add(key interface{}, value interface{}) {
	current, ok := lm.Map[key]
	if ok { // it's actually update, not new added value
		lm.Map[key] = mapValue{element: current.element, value: value}
		return
	}

	e := &Element{nil, nil, lm, key}
	lm.Map[key] = mapValue{element: e, value: value}

	if lm.first == nil {
		lm.first = e
		lm.last = e
		return
	}

	lm.last.next = e
	e.prev = lm.last
	lm.last = e
}

func (lm *LinkedMap) Get(key interface{}) interface{} {
	return lm.Map[key].value
}

func (lm *LinkedMap) GetWithOk(key interface{}) (interface{}, bool) {
	r, ok := lm.Map[key]
	if ok {
		return r.value, ok
	}
	return nil, ok
}

func (e *Element) Key() interface{} {
	return e.key
}

func (e *Element) Value() interface{} {
	return e.list.Get(e.key)
}

func (e *Element) Next() *Element {
	return e.next
}

func (e *Element) Prev() *Element {
	return e.prev
}

func (lm *LinkedMap) First() *Element {
	return lm.first
}

func (lm *LinkedMap) Last() *Element {
	return lm.last
}

func (lm *LinkedMap) Len() int {
	return len(lm.Map)
}

func (lm *LinkedMap) Delete(key interface{}) {
	mv, ok := lm.Map[key]
	if !ok {
		return
	}
	delete(lm.Map, key)
	if mv.element.prev != nil {
		mv.element.prev.next = mv.element.next
	} else {
		lm.first = mv.element.next
	}
	if mv.element.next != nil {
		mv.element.next.prev = mv.element.prev
	} else {
		lm.last = mv.element.prev
	}
}
