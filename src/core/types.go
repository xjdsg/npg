// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"strconv"
)

//postgres type oids
const (
	_BOOL             = 16
	_BYTEA            = 17
	_CHAR             = 18
	_NAME             = 19
	_INT8             = 20
	_INT2             = 21
	_INT2VECTOR       = 22
	_INT4             = 23
	_REGPROC          = 24
	_TEXT             = 25
	_OID              = 26
	_TID              = 27
	_XID              = 28
	_CID              = 29
	_OIDVECTOR        = 30
	_XML              = 142
	_POINT            = 600
	_LSEG             = 601
	_PATH             = 602
	_BOX              = 603
	_POLYGON          = 604
	_LINE             = 628
	_FLOAT4           = 700
	_FLOAT8           = 701
	_ABSTIME          = 702
	_RELTIME          = 703
	_TINTERVAL        = 704
	_UNKNOWN          = 705
	_CIRCLE           = 718
	_CASH             = 790
	_MACADDR          = 829
	_INET             = 869
	_CIDR             = 650
	_INT4ARRAY        = 1007
	_TEXTARRAY        = 1009
	_FLOAT4ARRAY      = 1021
	_ACLITEM          = 1033
	_CSTRINGARRAY     = 1263
	_BPCHAR           = 1042
	_VARCHAR          = 1043
	_DATE             = 1082
	_TIME             = 1083
	_TIMESTAMP        = 1114
	_TIMESTAMPTZ      = 1184
	_INTERVAL         = 1186
	_TIMETZ           = 1266
	_BIT              = 1560
	_VARBIT           = 1562
	_NUMERIC          = 1700
	_REFCURSOR        = 1790
	_REGPROCEDURE     = 2202
	_REGOPER          = 2203
	_REGOPERATOR      = 2204
	_REGCLASS         = 2205
	_REGTYPE          = 2206
	_REGTYPEARRAY     = 2211
	_TSVECTOR         = 3614
	_GTSVECTOR        = 3642
	_TSQUERY          = 3615
	_REGCONFIG        = 3734
	_REGDICTIONARY    = 3769
	_RECORD           = 2249
	_RECORDARRAY      = 2287
	_CSTRING          = 2275
	_ANY              = 2276
	_ANYARRAY         = 2277
	_VOID             = 2278
	_TRIGGER          = 2279
	_LANGUAGE_HANDLER = 2280
	_INTERNAL         = 2281
	_OPAQUE           = 2282
	_ANYELEMENT       = 2283
	_ANYNONARRAY      = 2776
	_ANYENUM          = 3500
)

func convert(ptype int, val string) interface{} {
	switch ptype {
	case _INT2, _INT4, _INT8:
		return tonumber(val)
	case _FLOAT4, _FLOAT8:
		fval, err := strconv.ParseFloat(val, 64)
		if err != nil { // Not expected
			panic(err)
		}
		return fval
	}
	return []byte(val)
}

func tonumber(val string) (number interface{}) {
	var err error
	if val[0] == '-' {
		number, err = strconv.ParseInt(val, 0, 64)
	} else {
		number, err = strconv.ParseUint(val, 0, 64)
	}
	if err != nil { // Not expected
		panic(err)
	}
	return number
}
