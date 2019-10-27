// Copyright 2019 HiWeb Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snowplow

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Contexts struct {
	Schema string         `json:"schema"`
	Data   []ContextsData `json:"data"`
}

type ContextsData struct {
	Schema string      `json:"schema"`
	Data   interface{} `json:"data"`
}

type Unstruct struct {
	Data UnstructData `json:"data"`
}

type UnstructData struct {
	Schema string      `json:"schema"`
	Data   interface{} `json:"data"`
}

type Schema struct {
	Vendor  string
	Name    string
	Format  string
	Version string
}

// SchemaURI ...
const SchemaURI = ("^iglu:" + // Protocol
	"([a-zA-Z0-9-_.]+)/" + // Vendor
	"([a-zA-Z0-9-_]+)/" + // Name
	"([a-zA-Z0-9-_]+)/" + // Format
	"([1-9][0-9]*" + // MODEL (cannot start with 0)
	"(?:-(?:0|[1-9][0-9]*)){2})$") // REVISION and ADDITION

var schemaURIRegex = regexp.MustCompile(SchemaURI)

// extractSchema Extracts Schema information from Iglu URI.
func extractSchema(uri string) (*Schema, error) {
	if matches := schemaURIRegex.FindStringSubmatch(uri); matches != nil {
		return &Schema{
			Vendor:  matches[1],
			Name:    matches[2],
			Format:  matches[3],
			Version: matches[4],
		}, nil
	}
	return nil, fmt.Errorf("Schema %s does not conform to regular expression %s", uri, SchemaURI)
}

// fixSchema Create an Elasticsearch field name from a schema string
func fixSchema(prefix, schema string) (string, error) {
	schemaDict, err := extractSchema(schema)
	if err != nil {
		return "", err
	}
	snakeCaseOrganization := strings.ToLower(strings.Replace(schemaDict.Vendor, ".", "_", -1))
	var re = regexp.MustCompile("([^A-Z_])([A-Z])")
	snakeCaseName := strings.ToLower(re.ReplaceAllString(schemaDict.Name, "${1}_${2}"))
	model := strings.Split(schemaDict.Version, "-")[0]
	return fmt.Sprintf("%s_%s_%s_%s", prefix, snakeCaseOrganization, snakeCaseName, model), nil
}

// parseContexts Convert a contexts JSON to an Elasticsearch-compatible list of key-value pairs
func parseContexts(data []byte) ([][]interface{}, error) {
	var contexts Contexts
	if err := json.Unmarshal(data, &contexts); err != nil {
		return nil, err
	}
	distinctContexts := map[string][]interface{}{}
	for _, c := range contexts.Data {
		schema, err := fixSchema("contexts", c.Schema)
		if err != nil {
			return nil, err
		}
		innerData := c.Data
		if val, ok := distinctContexts[schema]; !ok {
			distinctContexts[schema] = []interface{}{innerData}
		} else {
			distinctContexts[schema] = append(val, innerData)
		}
	}
	out := [][]interface{}{}
	for k, v := range distinctContexts {
		out = append(out, []interface{}{k, v})
	}
	return out, nil
}

func parseUnstruct(data []byte) ([][]interface{}, error) {
	var unstruct Unstruct
	if err := json.Unmarshal(data, &unstruct); err != nil {
		return nil, err
	}
	if unstruct.Data.Data == nil {
		return nil, errors.New("could not extract inner data field from unstructured event")
	}
	fixedSchema, err := fixSchema("unstruct_event", unstruct.Data.Schema)
	if err != nil {
		return nil, err
	}
	return [][]interface{}{{fixedSchema, unstruct.Data.Data}}, nil
}
