package util

import (
	"bufio"
	"encoding/csv"
	"os"
)

func ReadWords(path string) ([]string, error) {
	readFile, err := os.Open(path)
	if err != nil {
		return []string{}, err
	}

	defer readFile.Close()

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	var words []string
	for fileScanner.Scan() {
		words = append(words, fileScanner.Text())
	}
	return words, nil
}

func WriteCSV(path string, data [][]string) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	w := csv.NewWriter(f)
	err = w.WriteAll(data)
	if err != nil {
		panic(err)
	}
}
