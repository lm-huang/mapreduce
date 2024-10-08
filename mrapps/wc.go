package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "6.5840/mr"
import "unicode"
import "strings"
import "strconv"

// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
func Map(filename string, contents string) []mr.KeyValue {
	// 定义一个函数 ff，用于检测字符是否是非字母字符。
	// 如果字符不是字母（即为分隔符，如空格、标点符号等），该函数返回 true，表示可以用来分割单词。
	//每个 rune 对应一个 Unicode 码点，因此可以表示任何语言中的字符，不论是英文字母、汉字、表情符号等
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	//使用 strings.FieldsFunc 将文件内容按非字母字符分割成一个单词数组。
	//FieldsFunc 根据 ff 函数的逻辑来切割文本，将所有不属于字母的字符作为分隔符。
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values)) //strconv.Itoa 整数转字符串
}
