package definition

import "fmt"

// ConfigError is a startup failure that names the YAML field.
type ConfigError struct {
	Field  string
	Reason string
}

func (e *ConfigError) Error() string {
	if e.Reason == "" {
		return e.Field
	}
	return e.Field + ": " + e.Reason
}

func configErr(field, reason string) error {
	return &ConfigError{Field: field, Reason: reason}
}

func configf(field, format string, args ...any) error {
	return &ConfigError{Field: field, Reason: fmt.Sprintf(format, args...)}
}
