//bind variables to sql

package parser

import (
	"bytes"
	"encoding/json"
	"strconv"
)

type BindLocation struct {
	Offset, Length int
}

type ParsedQuery struct {
	Query         string
	BindLocations []BindLocation
}

type EncoderFunc func(value interface{}) ([]byte, error)

func (self *ParsedQuery) GenerateQuery(bindVariables map[string]interface{}, listVariables []Value) ([]byte, error) { //sqltypes.Value
	if len(self.BindLocations) == 0 {
		return []byte(self.Query), nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(self.Query)))
	current := 0
	for _, loc := range self.BindLocations {
		buf.WriteString(self.Query[current:loc.Offset])
		varName := self.Query[loc.Offset+1 : loc.Offset+loc.Length]
		var supplied interface{}
		if varName[0] >= '0' && varName[0] <= '9' {
			index, err := strconv.Atoi(varName)
			if err != nil {
				return nil, NewParserError("Unexpected: %v for %s", err, varName)
			}
			if index >= len(listVariables) {
				return nil, NewParserError("Index out of range: %d", index)
			}
			supplied = listVariables[index]
		} else {
			var ok bool
			supplied, ok = bindVariables[varName]
			if !ok {
				return nil, NewParserError("Missing bind var %s", varName)
			}
		}
		if err := EncodeValue(buf, supplied); err != nil {
			return nil, err
		}
		current = loc.Offset + loc.Length
	}
	buf.WriteString(self.Query[current:])
	return buf.Bytes(), nil
}

func (self *ParsedQuery) MarshalJSON() ([]byte, error) {
	return json.Marshal(self.Query)
}

func EncodeValue(buf *bytes.Buffer, value interface{}) error {
	switch bindVal := value.(type) {
	case nil:
		buf.WriteString("null")
	case []Value: //sqltypes.Value
		for i := 0; i < len(bindVal); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			if err := EncodeValue(buf, bindVal[i]); err != nil {
				return err
			}
		}
	case [][]Value: //sqltypes.Value
		for i := 0; i < len(bindVal); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteByte('(')
			if err := EncodeValue(buf, bindVal[i]); err != nil {
				return err
			}
			buf.WriteByte(')')
		}
	default:
		v, err := BuildValue(bindVal) //sqltypes
		if err != nil {
			return err
		}
		v.EncodeSql(buf)
	}
	return nil
}
