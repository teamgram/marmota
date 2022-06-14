## Doubly Linked Hash Table
This implementation uses built-in map, and extends it to track order in which (key, value) pairs are added.

This library, and built-in map type are not safe for concurrent use.

#### Example usage
```go
package main

import (
	"fmt"
	"github.com/mihgen/linkedmap"
)

func main() {
	lm := linkedmap.New()

	lm.Add(1, "value1")
	lm.Add(false, "value2")
	lm.Add("key3", "value3")
	lm.Add(false, "00updated00") // update doesn't change order

	// Show value of previously added k-v pair
	fmt.Println("Value of prev added k-v pair: ", lm.Last().Prev().Value())

	// Get value knowing a key
	fmt.Println("Value for key1 is", lm.Get(1))

	keys := []string{"key3", "no-key"}
	for _, k := range keys {
		v, ok := lm.GetWithOk(k) // returns false if key doesn't exist in a map
		fmt.Println("v, ok =", v, ",", ok)
	}

	// List all stored (k,v) pairs
	for e := lm.First(); e != nil; e = e.Next() {
		fmt.Println(e.Key(), "->", e.Value())
	}

	// Pairs before the first and after the last are nil
	fmt.Println("Before the first -", lm.First().Prev())
	fmt.Println("After the last -", lm.Last().Next())
}
```

#### Specifying the size of map
If it is required to specify allocation size for map, then new function can be added to linkedmap.go, and used instead of New():

```go
func NewAllocation(size int64) *LinkedMap {
	return &LinkedMap{Map: make(map[interface{}]mapValue, size)}
}
```
