package main

// cat src/build/yamls/antrea.yml | go run check-template.go

import (
	"fmt"
	"io/ioutil"
	"os"
	"text/template"
)

type templateVars struct {
	UseCertFromProvider    bool
	ProviderCertSecretName string
	ClusterIPCIDR          string
}

func main() {
	tmpl, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Errorf("Failed to read from stdin: %v", err)
	}
	t, err := template.New("t").Parse(string(tmpl))
	if err != nil {
		fmt.Errorf("Failed to parse template: %v", err)
	}
	vars := templateVars{true, "antrea-certificate", "10.96.0.0/12"}
	err = t.Execute(os.Stdout, vars)
	if err != nil {
		fmt.Errorf("Failed to render template: %v", err)
	}
}
