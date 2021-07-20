package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
)

const (
	helpText = `
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
-t and -p only function without -i`
)

var (
	linesProcessed    uint64
	inputFilepath     string
	inputFileBasename string
	outputFilepath    string
	delimiter         string

	logger = log.New(os.Stdout, "", log.LstdFlags)

	fs              = flag.NewFlagSet("csvclean", flag.ContinueOnError)
	commentPtr      = fs.String("c", "", "CSV comment start character")
	rawDelimiterPtr = fs.String("d", ",", "CSV value delimiter")
	encapsulatePtr  = fs.String("e", "\"", "Character to use for value encapsulation")
	headerPtr       = fs.Bool("h", false, "Mark input file as having a header")
	inPlacePtr      = fs.Bool("i", false, "Replace in place")
	outPermPtr      = fs.Uint("p", 0666, "Permission mask to set to output file if it must be created")
	truncatePtr     = fs.Bool("t", false, "Truncate output file prior to writing")
	verbosePtr      = fs.Bool("v", false, "Enable verbose logging")
)

func logit(debug bool, f string, v ...interface{}) {
	if debug && !*verbosePtr {
		return
	}
	logger.Printf(f, v...)
}

func parseDelimiter(in string) (string, error) {
	rs := []rune(in)
	rsl := len(rs)
	if rsl == 1 {
		return in, nil
	}
	if rsl == 2 && rs[1] == 't' {
		return "\t", nil
	}
	return "", fmt.Errorf("delimiter must be a single byte character, saw %b", rs)
}

func openFiles() (*os.File, *os.File, error) {
	var (
		inFile  *os.File
		inFlags int
		outFile *os.File
		err     error

		outFlags = os.O_CREATE | os.O_WRONLY
	)

	if *inPlacePtr {
		inFlags = os.O_RDWR
	} else {
		inFlags = os.O_RDONLY
		if *truncatePtr {
			outFlags |= os.O_TRUNC
		}
	}

	logit(true, "Using flags: input=%b; output=%b", inFlags, outFlags)
	logit(true, "Opening input file %q...", inputFilepath)

	if inFile, err = os.OpenFile(inputFilepath, inFlags, 0666); err != nil {
		return nil, nil, fmt.Errorf("error opening input file %q: %w", inputFilepath, err)
	}

	if *inPlacePtr {
		logit(true, "In-place overwrite specified, opening temp file...")
		outFile, err = os.CreateTemp(os.TempDir(), fmt.Sprintf("csvclean.*.%s", inputFileBasename))
		if err != nil {
			err = fmt.Errorf("error opening temporary filw: %q", err)
		}
	} else {
		logit(true, "Opening output file %q...", outputFilepath)
		outFile, err = os.OpenFile(outputFilepath, outFlags, (os.FileMode)(*outPermPtr))
		if err != nil {
			err = fmt.Errorf("error opening output file %q: %w", outputFilepath, err)
		}
	}
	return inFile, outFile, err
}

func processFile(inFile, outFile *os.File, stopChan <-chan struct{}) error {
	var (
		reader *csv.Reader
	)

	reader = csv.NewReader(inFile)
	reader.Comma = []rune(delimiter)[0]
	if len(*commentPtr) > 0 {
		reader.Comment = []rune(*commentPtr)[0]
	}

	for {
		var (
			inputLine   []string
			updatedLine []string
			err         error
		)
		if inputLine, err = reader.Read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error reading from input file %q: %w", inFile.Name(), err)
		}

		// increment line count
		atomic.AddUint64(&linesProcessed, 1)

		// check if we've been told to stop
		select {
		case <-stopChan:
			return nil
		default:
		}

		logit(true, "Processing input: %v", inputLine)

		if atomic.LoadUint64(&linesProcessed) == 1 && *headerPtr {
			updatedLine = inputLine
		} else {
			updatedLine = make([]string, len(inputLine))
			for i, value := range inputLine {
				updatedLine[i] = fmt.Sprintf("%s%s%s", *encapsulatePtr, value, *encapsulatePtr)
			}
		}

		logit(true, "Updated line: %v", updatedLine)

		if _, err = fmt.Fprintln(outFile, strings.Join(updatedLine, delimiter)); err != nil {
			return fmt.Errorf("error writing line %d to output file %q: %w", atomic.LoadUint64(&linesProcessed), outFile.Name(), err)
		}
	}
}

func cleanupTempFile(inFile, outFile *os.File) error {
	var err error
	if _, err = outFile.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking to beginning of temp file %q: %w", outFile.Name(), err)
	}
	if err = inFile.Truncate(0); err != nil {
		return fmt.Errorf("error truncating input file %q: %w", inputFilepath, err)
	}
	if _, err = io.Copy(inFile, outFile); err != nil {
		return fmt.Errorf("error overwriting input file %q with data from temp file %q: %w", inputFilepath, outFile.Name(), err)
	}
	return nil
}

func run(stopChan <-chan struct{}, errChan chan<- error) {
	var (
		inFile  *os.File
		outFile *os.File
		err     error
	)

	if inFile, outFile, err = openFiles(); err != nil {
		errChan <- err
		return
	}

	defer func() {
		if inFile != nil {
			_ = inFile.Close()
		}
		if outFile != nil {
			_ = outFile.Close()
		}
	}()

	if err = processFile(inFile, outFile, stopChan); err != nil {
		errChan <- err
		return
	}

	if *inPlacePtr {
		if err = cleanupTempFile(inFile, outFile); err != nil {
			errChan <- err
			return
		}
	}

	errChan <- nil
}

func main() {
	var (
		args     []string
		sigChan  chan os.Signal
		stopChan chan struct{}
		errChan  chan error
		err      error
	)

	if err = fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
	}

	args = fs.Args()
	switch len(args) {
	case 1:
		inputFilepath = args[0]
	case 2:
		inputFilepath, outputFilepath = args[0], args[1]
	default:
		fmt.Printf("Invalid input provided: %v", args)
		fmt.Println(helpText)
		os.Exit(1)
	}

	if delimiter, err = parseDelimiter(*rawDelimiterPtr); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	if len(*commentPtr) > 0 {
		if rs := []rune(*commentPtr); len(rs) > 1 {
			fmt.Printf("Comment marker must be single byte character, saw %b", rs)
			os.Exit(1)
		}
	}

	logit(true, "Using %b (%q) as delimiter", []rune(delimiter), delimiter)

	inputFileBasename = filepath.Base(inputFilepath)

	if outputFilepath == "" && !*inPlacePtr {
		bits := strings.SplitN(inputFileBasename, ".", 2)
		outputFilepath = path.Join(
			strings.Replace(
				inputFilepath,
				inputFileBasename,
				fmt.Sprintf("%s_clean.%s", bits[0], bits[1]),
				1,
			),
		)
	}

	sigChan = make(chan os.Signal, 1)
	stopChan = make(chan struct{})
	errChan = make(chan error, 1)

	signal.Notify(sigChan, os.Interrupt)

	go run(stopChan, errChan)

	select {
	case err = <-errChan:
		if err != nil {
			logit(false, "Error occurred during execution: %v", err)
			os.Exit(1)
		}
		logit(true, "Execution finished")
		os.Exit(0)
	case sig := <-sigChan:
		logit(false, "Processing interrupted (%s) after processing %d lines", sig, atomic.LoadUint64(&linesProcessed))
		close(stopChan)
		err = <-errChan
		if err != nil {
			logit(false, "Error occurred: %v", err)
			os.Exit(1)
		}
		logit(true, "Execution finished")
		os.Exit(0)
	}
}
