package cache

import (
	"fmt"
	"reflect"
	"testing"
)

type t_key int
type t_val string

type tentry struct {
	key  t_key
	val  t_val
	want t_val
	isok bool
}

func makeNEntries(n int) []tentry {
	var tentries []tentry
	for i := 0; i < n; i++ {
		key := t_key(i)
		val := t_val(fmt.Sprintf("value-%0.6d", i))
		tentries = append(tentries, tentry{
			key:  key,
			val:  val,
			want: val,
			isok: true,
		})
	}
	return tentries
}

func TestLRU_Set(t *testing.T) {
	tests := makeNEntries(64)
	for n, tt := range tests {
		t.Run("LRU_Set", func(t *testing.T) {
			l := NewLRU[t_key, t_val](128)
			got, got1 := l.Set(tt.key, tt.val)
			if n > 0 {
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Del() got = %v, want %v", got, tt.want)
				}
				if got1 != tt.isok {
					t.Errorf("Del() got1 = %v, want %v", got1, tt.isok)
				}
			}
		})
	}
}

func TestLRU_Get(t *testing.T) {
	tests := makeNEntries(64)
	for _, tt := range tests {
		t.Run("LRU_Get", func(t *testing.T) {
			l := NewLRU[t_key, t_val](128)
			got, got1 := l.Get(tt.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Del() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.isok {
				t.Errorf("Del() got1 = %v, want %v", got1, tt.isok)
			}
		})
	}
}

func TestLRU_Del(t *testing.T) {
	tests := makeNEntries(64)
	for _, tt := range tests {
		t.Run("LRU_Del", func(t *testing.T) {
			l := NewLRU[t_key, t_val](128)
			got, got1 := l.Del(tt.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Del() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.isok {
				t.Errorf("Del() got1 = %v, want %v", got1, tt.isok)
			}
		})
	}
}
