package numeric

import (
	"bytes"
	"errors"
	"unicode/utf8"
)

// rawText 是按“完整原始字节”保存和恢复的标识文本：提交人与请求号允许
// 包含任意 Go 字符串字节，包括 U+0000 与不能组成合法 UTF-8 的字节。
//
// 普通 encoding/json 把 string 当作 UTF-8 文本处理：非法字节在编码时
// 被替换为 U+FFFD，解码时无法复原——含非法 UTF-8 的标识落盘再打开后会
// 变成另一个字符串，原请求找不到自己的作业，原本不同的标识还可能塌缩到
// 同一条记录。rawText 自行实现 JSON 字符串的转义与反转义：除 UTF-8
// 文本外，把每个无法解码为符文的字节 b（均不小于 0x80）转义为码位区间
// U+FF80..U+FFFF 内的一个私有转义（低 8 位即原字节值），从而逐字节往返。
//
// 歧义说明：真正的 U+FFFD 符文以其 UTF-8 原始字节（0xEF 0xBF 0xBD）落盘，
// 不会使用私有转义；该转义区间与 UTF-16 代理项区间不相交，代理对仍按
// 标准规则解析。
type rawText string

// rawByteEscapePrefix 是非法字节私有转义的前缀（反斜杠 u f f 四个
// ASCII 字符），其后紧跟两位小写十六进制字节值。用字节拼接而不是字符串
// 字面量书写，避免源码中出现易被误读的转义文本。
var rawByteEscapePrefix = string([]byte{'\\', 'u', 'f', 'f'})

// rawByteEscapeMin/Max 界定私有原始字节转义的码位区间：码位
// U+FF80..U+FFFF 分别表示原始字节 0x80..0xFF。
const (
	rawByteEscapeMin rune = 0xFF80
	rawByteEscapeMax rune = 0xFFFF
)

// rawMarshal 以调用方给出的完整字节构造落盘标识。
func rawMarshal(s string) rawText { return rawText(s) }

// String 按完整字节还原为 Go 字符串标识。
func (r rawText) String() string { return string(r) }

// MarshalJSON 把原始字节编码为 JSON 字符串。合法 UTF-8 符文原样输出
// （引号、反斜杠与控制字符按 JSON 规则转义）；不能组成合法 UTF-8 的单个
// 字节输出为 rawByteEscapePrefix 加两位十六进制值，如字节 0xFF 编码为
// 码位 U+FFFF 的转义。
func (r rawText) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.Grow(len(r) + 2)
	b.WriteByte('"')
	s := []byte(r)
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '"':
			b.WriteString(`\"`)
			i++
		case c == '\\':
			b.WriteString(`\\`)
			i++
		case c == '\n':
			b.WriteString(`\n`)
			i++
		case c == '\r':
			b.WriteString(`\r`)
			i++
		case c == '\t':
			b.WriteString(`\t`)
			i++
		case c < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte(hexDigit(c >> 4))
			b.WriteByte(hexDigit(c & 0x0f))
			i++
		case c < 0x80:
			b.WriteByte(c)
			i++
		default:
			// 多字节序列：能解码出合法符文就原样保留其 UTF-8 字节；
			// 非法首字节或残缺序列（size 等于 1）按单字节转义，后续的
			// 孤立续字节同样逐个转义。
			_, size := utf8.DecodeRune(s[i:])
			if size > 1 {
				b.Write(s[i : i+size])
				i += size
			} else {
				writeRawByteEscape(&b, c)
				i++
			}
		}
	}
	b.WriteByte('"')
	return b.Bytes(), nil
}

// writeRawByteEscape 写出单个非法字节的私有转义（前缀 + 两位十六进制）。
func writeRawByteEscape(b *bytes.Buffer, c byte) {
	b.WriteString(rawByteEscapePrefix)
	b.WriteByte(hexDigit((c >> 4) & 0x0f))
	b.WriteByte(hexDigit(c & 0x0f))
}

// UnmarshalJSON 解析 rawText 写出的 JSON 字符串：私有码位转义还原为
// 原始单字节，普通 UTF-8 文本与标准 JSON 转义（含代理对）照常解析。
func (r *rawText) UnmarshalJSON(data []byte) error {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errRawTextNotString
	}
	var b bytes.Buffer
	b.Grow(len(data))
	s := data[1 : len(data)-1]
	for i := 0; i < len(s); {
		c := s[i]
		if c != '\\' {
			b.WriteByte(c)
			i++
			continue
		}
		if i+1 >= len(s) {
			return errRawTextTruncated
		}
		switch s[i+1] {
		case '"', '\\', '/':
			b.WriteByte(s[i+1])
			i += 2
		case 'b':
			b.WriteByte('\b')
			i += 2
		case 'f':
			b.WriteByte('\f')
			i += 2
		case 'n':
			b.WriteByte('\n')
			i += 2
		case 'r':
			b.WriteByte('\r')
			i += 2
		case 't':
			b.WriteByte('\t')
			i += 2
		case 'u':
			rn, n, err := decodeUnicodeEscape(string(s[i:]))
			if err != nil {
				return err
			}
			if rn >= rawByteEscapeMin && rn <= rawByteEscapeMax {
				// 本包私有转义：码位低 8 位即原始字节（非法 UTF-8 字节
				// 均不小于 0x80，与代理项区间不相交）。
				b.WriteByte(byte(rn))
			} else {
				var out [4]byte
				m := utf8.EncodeRune(out[:], rn)
				b.Write(out[:m])
			}
			i += n
		default:
			return errRawTextBadEscape
		}
	}
	*r = rawText(b.Bytes())
	return nil
}

// decodeUnicodeEscape 解析从 s 起始处的一个反斜杠 u XXXX 转义；遇到
// UTF-16 高代理项且其后紧跟合法低代理项时合并解析。孤立代理项按
// encoding/json 的既有行为替换为 U+FFFD。返回符文与消耗字节数。
func decodeUnicodeEscape(s string) (rune, int, error) {
	v, ok := parseHex4(s)
	if !ok {
		return 0, 0, errRawTextBadEscape
	}
	if v < 0xD800 || v > 0xDFFF {
		return rune(v), 6, nil
	}
	if v <= 0xDBFF && len(s) >= 12 {
		if lo, ok := parseHex4(s[6:]); ok && lo >= 0xDC00 && lo <= 0xDFFF {
			return rune(0x10000 + (v-0xD800)<<10 + (lo - 0xDC00)), 12, nil
		}
	}
	// 孤立高/低代理项：与标准解码器一致地用 U+FFFD 替换，只消耗本转义。
	return utf8.RuneError, 6, nil
}

// parseHex4 要求 s 以反斜杠 u 开头并解析随后四位十六进制值。
func parseHex4(s string) (uint32, bool) {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return 0, false
	}
	var v uint32
	for k := 2; k < 6; k++ {
		c := s[k]
		switch {
		case c >= '0' && c <= '9':
			v = v<<4 | uint32(c-'0')
		case c >= 'a' && c <= 'f':
			v = v<<4 | uint32(c-'a'+10)
		case c >= 'A' && c <= 'F':
			v = v<<4 | uint32(c-'A'+10)
		default:
			return 0, false
		}
	}
	return v, true
}

func hexDigit(c byte) byte {
	if c < 10 {
		return '0' + c
	}
	return 'a' + c - 10
}

var (
	errRawTextNotString = errors.New("numeric: 标识字段不是 JSON 字符串")
	errRawTextTruncated = errors.New("numeric: 标识字符串的转义被截断")
	errRawTextBadEscape = errors.New("numeric: 标识字符串含非法转义")
)
