// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
//"fmt"
)

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
