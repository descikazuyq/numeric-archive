package numeric

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRawTextRoundTripsArbitraryBytes(t *testing.T) {
	cases := []string{
		"",
		"alice",
		"提交人-甲",
		"a\x00b",
		"\x00",
		"\xff",
		"\xfe",
		"�", // 真正的替换字符 U+FFFD（UTF-8: ef bf bd）
		"a\xffb\xfec",
		"中\xff文\xfe\x00",
		// 孤立续字节、非法首字节、残缺序列、overlong。
		"\x80\xbf\xc0\xc1\xe0\x80",
		// 合法多字节序列与非法字节相邻。
		" \xc2\xa0\xff",
		// JSON 标准转义涉及的字符。
		"\"\\\n\r\t\x01\x1f",
	}
	for _, in := range cases {
		data, err := json.Marshal(rawMarshal(in))
		if err != nil {
			t.Fatalf("marshal %q: %v", in, err)
		}
		// 输出必须是合法 JSON 文档（能被标准解码器当作字符串读入）。
		var probe string
		if err := json.Unmarshal(data, &probe); err != nil {
			t.Fatalf("encoding %q produced invalid JSON %s: %v", in, data, err)
		}
		var got rawText
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %q: %v", in, err)
		}
		if got.String() != in {
			t.Fatalf("round-trip mismatch:\n in=% x\nout=% x\njson=%s", in, got.String(), data)
		}
	}
}

func TestRawTextDistinguishesRawBytesFromReplacementRune(t *testing.T) {
	// 三个互相不同的非空标识必须编码成三个不同的 JSON 值，并且各自往返。
	ids := []string{"\xff", "\xfe", "�"}
	seen := make(map[string]string, len(ids)) // encoded JSON -> decoded value
	for _, id := range ids {
		data, err := json.Marshal(rawMarshal(id))
		if err != nil {
			t.Fatalf("marshal % x: %v", id, err)
		}
		if _, dup := seen[string(data)]; dup {
			t.Fatalf("distinct identifiers encoded identically: %s", data)
		}
		seen[string(data)] = id
	}
	for enc, want := range seen {
		var got rawText
		if err := json.Unmarshal([]byte(enc), &got); err != nil {
			t.Fatalf("unmarshal %s: %v", enc, err)
		}
		if !bytes.Equal([]byte(got.String()), []byte(want)) {
			t.Fatalf("%s decoded to % x, want % x", enc, got.String(), want)
		}
	}
}

func TestRawTextRejectsMalformedJSON(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`"`),      // 缺右引号
		[]byte(`"abc\`),  // 结尾孤立反斜杠
		[]byte(`"\q"`),   // 非法转义
		[]byte(`"\u00"`), // 十六进制不足
		[]byte(`123`),    // 非字符串
	} {
		var got rawText
		if err := json.Unmarshal(data, &got); err == nil {
			t.Fatalf("invalid input %s accepted as %q", data, got)
		}
	}
}

// TestRawTextEmbeddedInStruct 验证 rawText 作为结构体字段经标准
// encoding/json 编解码时同样逐字节往返（这是作业记录实际使用的路径）。
func TestRawTextEmbeddedInStruct(t *testing.T) {
	type holder struct {
		Submitter rawText `json:"submitter"`
		RequestID rawText `json:"request_id"`
	}
	h := holder{Submitter: rawMarshal("s\xff\x00中"), RequestID: rawMarshal("\xfe")}
	data, err := json.Marshal(&h)
	if err != nil {
		t.Fatal(err)
	}
	var got holder
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Submitter.String() != h.Submitter.String() ||
		got.RequestID.String() != h.RequestID.String() {
		t.Fatalf("struct round-trip: %q,%q", got.Submitter, got.RequestID)
	}
}
