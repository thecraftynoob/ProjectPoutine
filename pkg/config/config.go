// Package config provides a minimal, dependency-light environment-variable
// config loader. Each service defines its own typed config struct (e.g.
// GRPCPort, PostgresDSN, RedisAddr, NATSURL, TenantID where relevant) with
// `env:"NAME"` struct tags, and calls Load[T]() to populate it.
//
// This is a small hand-rolled loader (reflection over struct tags) rather
// than pulling in github.com/caarlos0/env, keeping the dependency graph
// minimal for what is, today, just a handful of scalar fields per service.
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
)

// Load populates a new T from environment variables, using each field's
// `env:"NAME"` struct tag to determine which variable to read. An
// `envDefault:"value"` tag supplies a fallback when the variable is unset
// or empty. Supported field kinds: string, int (and sized variants), bool.
//
// A field tagged `env:"NAME,required"` returns an error if the variable is
// unset/empty and no envDefault is present.
func Load[T any]() (T, error) {
	var cfg T
	v := reflect.ValueOf(&cfg).Elem()
	if v.Kind() != reflect.Struct {
		return cfg, fmt.Errorf("config: Load[T] requires T to be a struct, got %s", v.Kind())
	}

	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag, ok := field.Tag.Lookup("env")
		if !ok || tag == "" {
			continue
		}

		name := tag
		required := false
		// Support "NAME,required" without pulling in strings.Split for one case.
		for j := 0; j < len(tag); j++ {
			if tag[j] == ',' {
				name = tag[:j]
				if tag[j+1:] == "required" {
					required = true
				}
				break
			}
		}

		raw, present := os.LookupEnv(name)
		if !present || raw == "" {
			if def, hasDefault := field.Tag.Lookup("envDefault"); hasDefault {
				raw = def
			} else if required {
				return cfg, fmt.Errorf("config: required environment variable %s is not set", name)
			} else {
				continue
			}
		}

		fv := v.Field(i)
		if err := setField(fv, raw); err != nil {
			return cfg, fmt.Errorf("config: field %s (env %s): %w", field.Name, name, err)
		}
	}

	return cfg, nil
}

func setField(fv reflect.Value, raw string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	default:
		return fmt.Errorf("unsupported field kind %s", fv.Kind())
	}
	return nil
}
