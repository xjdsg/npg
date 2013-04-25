// Copyright 2010 The go-pgsql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"container/list"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	//"log"
	"strconv"
	"strings"
)

func (conn *Conn) read(b []byte) {
	readTotal := 0
	for {
		n, err := conn.reader.Read(b[readTotal:])
		panicIfErr(err)

		readTotal += n
		if readTotal == len(b) {
			break
		}
	}
}

func (conn *Conn) readNbyte(n int32) []byte {
	b := make([]byte, n)
	conn.read(b)
	return b
}

func (conn *Conn) readByte() byte {
	b, err := conn.reader.ReadByte()
	panicIfErr(err)

	return b
}

func (conn *Conn) readBytes(delim byte) []byte {
	b, err := conn.reader.ReadBytes(delim)
	panicIfErr(err)

	return b
}

func (conn *Conn) readInt16() int16 {
	var buf [2]byte
	b := buf[:]

	conn.read(b)
	return int16(binary.BigEndian.Uint16(b))
}

func (conn *Conn) readInt32() int32 {
	var buf [4]byte
	b := buf[:]

	conn.read(b)
	return int32(binary.BigEndian.Uint32(b))
}

func (conn *Conn) readString() string {
	b := conn.readBytes(0)
	return string(b[:len(b)-1])
}

func (conn *Conn) readAuth() {
    conn.readByte() //read cmd
	//log.Println("Auth: ", string(conn.readByte())) //to eat command
	// Just eat message length.
	conn.readInt32()

	authType := conn.readInt32()
	switch authenticationType(authType) {
	case _AuthenticationOk:
		// nop

		//		case _AuthenticationKerberosV5 authenticationType:

		//		case _AuthenticationCleartextPassword:

	case _AuthenticationMD5Password:
		salt := make([]byte, 4)

		conn.read(salt)

		md5Hasher := md5.New()

		_, err := md5Hasher.Write([]byte(conn.params.Password))
		panicIfErr(err)

		_, err = md5Hasher.Write([]byte(conn.params.User))
		panicIfErr(err)

		md5HashHex1 := hex.EncodeToString(md5Hasher.Sum(nil))

		md5Hasher.Reset()

		_, err = md5Hasher.Write([]byte(md5HashHex1))
		panicIfErr(err)

		_, err = md5Hasher.Write(salt)
		panicIfErr(err)

		md5HashHex2 := hex.EncodeToString(md5Hasher.Sum(nil))

		password := "md5" + md5HashHex2

		conn.writePasswordMessage(password)

		//		case _AuthenticationSCMCredential:

		//		case _AuthenticationGSS:

		//		case _AuthenticationGSSContinue:

		//		case _AuthenticationSSPI:

	default:
		panic(fmt.Sprintf("unsupported authentication type: %d", authType))
	}
}

func (conn *Conn) getResultComplete() (rowsAffected int64) {
	// Just eat message length.
	conn.readInt32()

	// Retrieve the number of affected rows from the command tag.
	tag := conn.readString()
	parts := strings.Split(tag, " ")
	rowsAffected, _ = strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return
}

func (conn *Conn) getFields() (fields []Field) {

	// Just eat message length.
	conn.readInt32()

	fieldCount := conn.readInt16()

	fields = make([]Field, fieldCount)
	for i := 0; i < int(fieldCount); i++ {
		fields[i].Name = conn.readString()
		// Just eat table OID.
		conn.readInt32()

		// Just eat field OID.
		conn.readInt16()

		fields[i].Type = conn.readInt32()

		// Just eat field size.
		conn.readInt16()

		// Just eat field type modifier.
		conn.readInt32()
		// Just eat field format: textFormat,binaryFormat
		conn.readInt16()
	}
	return
}

func (conn *Conn) getRow() (row []Value) {
	// Just eat message length.
	conn.readInt32()

	fieldCount := conn.readInt16()

	row = make([]Value, fieldCount)

	for i := 0; i < int(fieldCount); i++ {
		valLen := conn.readInt32()

		var val []byte

		if valLen == -1 {
			val = nil
		} else {
			val = make([]byte, valLen)
			conn.read(val)
		}
		row[i] = Value{raw: val}
		//fmt.Println("val", string(val))
	}
	return
}

func (conn *Conn) getResult() (rs *Result) {
	rs = &Result{qr: &QueryResult{}}
	rlist := list.New()

	for {
		cmd := conn.readByte()
		//fmt.Println("cmd: ", string(cmd))
		switch cmd {
		case 'T':
			rs.qr.Fields = conn.getFields()
			//fmt.Println("Fields:", rs.qr.Fields)
		case 'D':
			rlist.PushBack(conn.getRow())
		case 'C':
			rs.qr.RowsAffected = uint64(conn.getResultComplete())
			rlen := rlist.Len()
			rows := make([][]Value, rlen)
			for i := 0; i < rlen; i++ {
				rows[i] = rlist.Remove(rlist.Front()).([]Value)
			}
			fmt.Println("rows : ", rows)
			rs.qr.Rows = rows

			return
		default:
			n := conn.readInt32()
            //eat the msg
            conn.readNbyte(n-4)
			//fmt.Println("msg:", n, string(conn.readNbyte(n-4))) //4 is the len of n
		}
	}

	return
}

func (conn *Conn) getPreparedStmt(st *Stmt) {
	for {
		cmd := conn.readByte()
		//fmt.Println("cmd: ", string(cmd))
		switch cmd {
		case 't': //_ParameterDescription
			conn.readInt32()
			nparams := conn.readInt16()
            //fmt.Println("nparams: ", nparams)
            st.params = make([]*stParams, nparams)
			for i := 0; i < int(nparams); i++ {
                st.params[i] =&stParams{ptype: conn.readInt32()}  //fix
			}
            return //fix
/*        case 'T':
            n := conn.readInt32()
			fmt.Println("msg:", n, string(conn.readNbyte(n-4))) //4 is the len of n
            return
*/
		default:
            n := conn.readInt32()
            //eat the msg
            conn.readNbyte(n-4)
			//fmt.Println("msg:", n, string(conn.readNbyte(n-4))) //4 is the len of n
		}
	}
	return
}
/*
//parse the command info from backend
func (conn *Conn) readBMsg() interface{} {
	for {
		msgCode := backendMessageCode(conn.readByte())
		log.Println("cmd : ", msgCode)

		switch msgCode {
		case _AuthenticationRequest: //R
			conn.readAuth()

		case _BackendKeyData: //K
			conn.readInt32()
			conn.readInt32() //backendPid
			conn.readInt32() //backendSecretKey 

		case _BindComplete:
			conn.readInt32()

		case _CloseComplete:
			conn.readInt32()

		case _CommandComplete: //C
			return conn.getResultComplete()

		case _CopyInResponse:
			conn.readCopyInResponse()

		case _DataRow: //D
			return conn.getRow()

		case _EmptyQueryResponse: //I
			conn.readInt32()

		case _ErrorResponse:
			n := conn.readInt32()
			conn.readNbyte(n)
			//conn.readErrorOrNoticeResponse(true)

		case _NoData:
			conn.readInt32()

		case _NoticeResponse:
			n := conn.readInt32()
			conn.readNbyte(n)
			//conn.readErrorOrNoticeResponse(false)

		case _ParameterStatus: //S
			conn.readInt32()
			log.Println("S name:", conn.readString())
			log.Println("S value:", conn.readString())

		case _ParseComplete:
			conn.readInt32()

		case _ReadyForQuery: //Z
			conn.readInt32()
			conn.readByte() //TxStatus 

		case _RowDescription: //T
			return conn.getFields()
		}

	}
	return nil

}

*/
