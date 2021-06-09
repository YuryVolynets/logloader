package config

import (
	"io/ioutil"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Data struct contains params for app configuration
type Data struct {
	CMSDomains map[string]struct{}
	Paths      struct {
		Site     string
		CMS      string
		Unsorted string
	}
	WriteLimit      int
	DomainValidator *regexp.Regexp
}

// Load config from yaml at specified path
func Load(fn string) (Data, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return Data{}, err
	}

	var c struct {
		CMSDomains []string `yaml:"cms_domains"`
		Paths      struct {
			Site     string
			CMS      string
			Unsorted string
		}
		WriteLimit      int    `yaml:"write_limit"`
		DomainValidator string `yaml:"domain_validator"`
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Data{}, err
	}

	return Data{
		CMSDomains:      sliceToMap(c.CMSDomains),
		Paths:           c.Paths,
		WriteLimit:      c.WriteLimit,
		DomainValidator: regexp.MustCompile(c.DomainValidator),
	}, nil
}

func sliceToMap(sl []string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, s := range sl {
		m[s] = struct{}{}
	}
	return m
}
