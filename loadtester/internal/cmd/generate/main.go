//go:generate go run main.go
package main

import (
	"bytes"
	_ "embed"
	"go/format"
	"os"
	"text/template"
)

//go:embed gen_strategies.go.tmpl
var templateBody string

func main() {
	tmpl, err := template.New("").Parse(templateBody)
	if err != nil {
		panic(err)
	}

	const dstFile = "../../../gen_strategies.go"

	defer func() {
		if r := recover(); r != nil {
			defer panic(r)
			os.Remove(dstFile)
			return
		}
	}()

	f, err := os.Create(dstFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var buf bytes.Buffer

	type data struct {
		ImportsOnly    bool
		GenDoTask      bool
		RetriesEnabled bool
		MaxTasksGTZero bool
	}

	_, err = buf.WriteString(`// Code generated by ./internal/cmd/generate/main.go DO NOT EDIT.` + "\n\n")
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(&buf, data{
		ImportsOnly: true,
	})
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(&buf, data{
		RetriesEnabled: true,
		GenDoTask:      true,
		MaxTasksGTZero: false,
	})
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(&buf, data{
		RetriesEnabled: false,
		GenDoTask:      true,
		MaxTasksGTZero: false,
	})
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(&buf, data{
		RetriesEnabled: true,
		GenDoTask:      false,
		MaxTasksGTZero: true,
	})
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(&buf, data{
		RetriesEnabled: false,
		GenDoTask:      false,
		MaxTasksGTZero: true,
	})
	if err != nil {
		panic(err)
	}

	b, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	_, err = f.Write(b)
	if err != nil {
		panic(err)
	}
}
