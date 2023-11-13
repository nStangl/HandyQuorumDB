package util

import (
	"strings"

	"github.com/spf13/pflag"
)

func ExtractUnknownArgs(flags *pflag.FlagSet, args []string) []string {
	var unknownArgs []string

	for i := 0; i < len(args); i++ {
		var (
			f *pflag.Flag
			a = args[i]
		)

		if a[0] == '-' {
			if a[1] == '-' {
				f = flags.Lookup(strings.SplitN(a[2:], "=", 2)[0])
			} else {
				for _, s := range a[1:] {
					f = flags.ShorthandLookup(string(s))
					if f == nil {
						break
					}
				}
			}
		}

		if f != nil {
			if f.NoOptDefVal == "" && i+1 < len(args) && f.Value.String() == args[i+1] {
				i++
			}

			continue
		}

		unknownArgs = append(unknownArgs, a)
	}

	return unknownArgs
}
