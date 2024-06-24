package migrations

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"
)

// MigrationNameConvention represents a naming convention in terms of
// casing and underscores for a Migrator.
type MigrationNameConvention string

const (
	// CamelCase represents a camelCase naming convention.
	CamelCase MigrationNameConvention = "camelCase"

	// SnakeCase represents a snake_case naming convention.
	SnakeCase MigrationNameConvention = "snakeCase"
)

var (
	// ErrUnknownNamingConvention indicates that an attempt was made to
	// create a migration, without specifying a name.
	ErrUnknownNamingConvention = errors.New("unknown naming convention")
)

// ConvertCamelCaseToSnakeCase converts a potentially camel-case
// string to snake-case. Should be Unicode-safe.
//
// Spaces are converted to underscores and any uppercase letters
// are replaced with an underscore and the lowercase version of
// the same letter.
func ConvertCamelCaseToSnakeCase(word string) (result string) {
	if len(word) == 0 {
		return ""
	}

	var err error
	builder := &strings.Builder{}
	char, _ := utf8.DecodeRuneInString(word)
	_, err = builder.WriteRune(unicode.ToLower(char))
	if err != nil {
		panic(err)
	}

	var prevWordBoundary bool
	for _, char := range word[1:] {
		if char == '_' || unicode.IsSpace(char) {
			prevWordBoundary = true
			continue
		}
		if prevWordBoundary || unicode.IsUpper(char) {
			prevWordBoundary = false
			_, err = builder.WriteRune('_')
			if err != nil {
				panic(err)
			}
		}

		_, err = builder.WriteRune(unicode.ToLower(char))
		if err != nil {
			panic(err)
		}
	}

	return builder.String()
}

// ConvertSnakeCaseToCamelCase converts a potentially snake-case
// string to camel-case. Should be Unicode-safe.
//
// Spaces and underscores are removed and any letter following
// immediately after these removed characters will be converted
// to uppercase.
func ConvertSnakeCaseToCamelCase(word string) (result string) {
	builder := &strings.Builder{}
	var prevWordBoundary bool
	for _, char := range word {
		if char == '_' || unicode.IsSpace(char) {
			prevWordBoundary = true
			continue
		}

		var err error
		if prevWordBoundary {
			prevWordBoundary = false
			_, err = builder.WriteRune(unicode.ToUpper(char))
		} else {
			_, err = builder.WriteRune(unicode.ToLower(char))
		}
		if err != nil {
			panic(err)
		}
	}

	return builder.String()
}

// Caser is intended to convert a description to a particular naming
// convention. It should take spaces into account.
type Caser interface {
	// ToFileCase converts a description to the casing required for
	// a filename. There should not be any spaces in the output.
	ToFileCase(time.Time, string) string

	// ToFileCase converts a description to the casing required for
	// a function name. There should not be any spaces in the output.
	ToFuncCase(time.Time, string) string
}

// SnakeCaser will attempt to use snake_case for filenames.
type SnakeCaser struct{}

// Interface Compliance: This ensures compile-time checks
// that SnakeCaser indeed implements all methods of Caser.
var _ Caser = (*SnakeCaser)(nil)

func (x SnakeCaser) ToFileCase(date time.Time, input string) string {
	// Panicking here is acceptable, because builder.WriteString
	// should only ever return an error when out of memory.
	builder := strings.Builder{}
	description := ConvertCamelCaseToSnakeCase(fmt.Sprintf("%s %s", date.Format("20060102150405"), input))
	_, err := builder.WriteString(description)
	if err != nil {
		panic(err)
	}
	return builder.String()
}

func (x SnakeCaser) ToFuncCase(date time.Time, input string) string {
	// Panicking here is acceptable, because builder.WriteString
	// should only ever return an error when out of memory.
	builder := strings.Builder{}
	description := ConvertSnakeCaseToCamelCase(fmt.Sprintf("%s %s", date.Format("20060102150405"), input))
	_, err := builder.WriteString(description)
	if err != nil {
		panic(err)
	}
	return builder.String()
}

// CamelCaser will attempt to use camelCase for filenames.
type CamelCaser struct{}

// Interface Compliance: This ensures compile-time checks
// that CamelCaser indeed implements all methods of Caser.
var _ Caser = (*CamelCaser)(nil)

func (x CamelCaser) ToFileCase(date time.Time, input string) string {
	// Panicking here is acceptable, because builder.WriteString
	// should only ever return an error when out of memory.
	builder := strings.Builder{}
	description := ConvertSnakeCaseToCamelCase(fmt.Sprintf("%s %s", date.Format("20060102150405"), input))
	_, err := builder.WriteString(description)
	if err != nil {
		panic(err)
	}
	return builder.String()
}

func (x CamelCaser) ToFuncCase(date time.Time, input string) string {
	// Panicking here is acceptable, because builder.WriteString
	// should only ever return an error when out of memory.
	builder := strings.Builder{}
	description := ConvertSnakeCaseToCamelCase(fmt.Sprintf("%s %s", date.Format("20060102150405"), input))
	_, err := builder.WriteString(description)
	if err != nil {
		panic(err)
	}
	return builder.String()
}

// GetCaser returns the appropriate caser for the given naming convention.
func GetCaser(convention MigrationNameConvention) (Caser, error) {
	switch convention {
	case SnakeCase:
		return SnakeCaser{}, nil
	case CamelCase:
		return CamelCaser{}, nil
	default:
		err := errors.Wrapf(
			ErrUnknownNamingConvention,
			"unknown convention %s",
			convention,
		)
		return nil, err
	}
}
