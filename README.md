# csvclean
Little go program to clean a character separated file

## Basic usage
```
csvclean - simple character separated value escape utility

Usage:
	csvclean [options] infile [outfile]

Options:
	-c		Character comments are started with
	-d		Character values are separated by
	-e		Character to encapsulate values with
	-h		Mark the input file as having a header
	-i		Overwrite source file with updated contents
	-p		Output file permission mask
	-t		Truncate output file prior to writing
	-v		Enable verbose logging

If -i is specified, outfile may not be specified
If -i is NOT specified, outfile defaults to infile_clean.ext
-t and -p only function without -i
```

Functionality will be limited to what I need it to do.