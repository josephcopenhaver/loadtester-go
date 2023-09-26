//go:generate go run main.go
package main

import (
	_ "embed"
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

	const dstFile = "../../gen_strategies.go"

	defer func() {
		if r := recover(); r != nil {
			defer panic(r)
			os.Remove(dstFile)
			return
		}

		// TODO: run autoformatter
	}()

	f, err := os.Create(dstFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	type data struct {
		ImportsOnly    bool
		RetriesEnabled bool
	}

	err = tmpl.Execute(f, data{
		ImportsOnly: true,
	})
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(f, data{
		RetriesEnabled: true,
	})
	if err != nil {
		panic(err)
	}

	err = tmpl.Execute(f, data{
		RetriesEnabled: false,
	})
	if err != nil {
		panic(err)
	}
}
