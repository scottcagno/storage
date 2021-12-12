//go:build gc && go1.9
// +build gc,go1.9

package rawalloc

import "unsafe"

// The go:linkname directives provides backdoor access to private functions in
// the runtime. Below we're accessing the mallocgc function. Note that this
// access is necessarily tied to a specific Go release which is why this file
// is protected by a build tag.

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer
