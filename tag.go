package questdb

import (
	"fmt"
	"strings"
)

const tagName = "qdb"

// ensureOptionsAreValid func will take a option tags []string and check and make sure
// each one being set is valid. If not, it will return an error.
func ensureOptionsAreValid(opts []string) error {
	for _, v := range opts {
		vSplit := strings.Split(v, ":")
		if len(vSplit) != 2 {
			return fmt.Errorf("'%s' is not valid option", v)
		}
		// possibly check against valid options keys and values in opts []string in future?
	}
	return nil
}

// getOption func will take a slice of strings (tag options) and a string representing an option
// thats trying to be extracted and will attempt to find and return that options set value.
// If that option is not set in the struct field, it will return an empty string ("").
func getOption(opts []string, option string) string {
	for _, v := range opts {
		vSplit := strings.Split(v, ":")
		optName := vSplit[0]
		optVal := vSplit[1]
		if option == optName {
			return optVal
		}
	}
	return ""
}

// tagOptions struct represents options set by the tag of a specific struct field.
type tagOptions struct {
	embeddedPrefix  string
	designatedTS    bool
	commitZeroValue bool
}

// makeTagOptions func takes a tagOpts []string and returns a tagOptions struct
func makeTagOptions(f *field, tagsOpts []string) (tagOptions, error) {
	opts := tagOptions{
		embeddedPrefix: "",
	}

	// embeddedPrefix
	embeddedPrefix := getOption(tagsOpts, "embeddedPrefix")
	if embeddedPrefix != "" {
		opts.embeddedPrefix = embeddedPrefix
	}

	// designated ts fields
	isDesignatedTSField := getOption(tagsOpts, "designatedTS")
	if isDesignatedTSField == "true" {
		if f.qdbType != Timestamp {
			return opts, fmt.Errorf("type must be timestamp if 'designatedTS:true' option set")
		}
		opts.designatedTS = true
	}

	// commit zero values
	commitZeroValueField := getOption(tagsOpts, "commitZeroValue")
	if commitZeroValueField == "true" {
		opts.commitZeroValue = true
	}

	return opts, nil
}
