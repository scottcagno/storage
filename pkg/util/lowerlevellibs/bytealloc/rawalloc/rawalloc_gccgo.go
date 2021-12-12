//go:build gccgo
// +build gccgo

package rawalloc

import "unsafe"

//extern runtime.mallocgc
//func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer
