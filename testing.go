package main

import (
	//"bufio"
	"fmt"
	//"io/ioutil"
	"log"
	//"net"
	"os"
	"regexp"
	"strings"
)

// Keeps track of where a chunk of words have been processed and a slice of the words themselves
type ChunkInfo struct {
	processed bool     //Has this chunk been processed?
	words     []string //words to process
}

// Divides one input file into 10 chunks
func divide2(files []string) {
	var chunks [10]ChunkInfo     //array of slices
	words := readFile2(files[0]) //*assuming there is only one file in files* Get a list of all the words in the file
	totalWords := len(words)     //total number of words in input
	chunkSize := totalWords / 10 //expected number of words per chunk
	//*The last mapper may have nearly twice as much work as other mappers (uneven split)
	for i := 0; i < 9; i++ { //loop 10 times
		if len(words[i*chunkSize]) > 0 { //just in case we have a very small input file
			var info = new(ChunkInfo)
			info.processed = false
			info.words = words[i*chunkSize : (i+1)*chunkSize]
			chunks[i] = *info
		}

	}
	//for the remainder
	var info = new(ChunkInfo)
	info.processed = false
	info.words = words[9*chunkSize:]
	chunks[9] = *info
	//now we have 10 chunks of words
	fmt.Println(chunks[9])
}

// Given a file name, opens that file and returns an array of bytes.
// Logs if there is an array opening and reading the file.
func readFile2(filename string) []string {
	input, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("Error opening file: ", err)
	}
	reg, err := regexp.Compile("[^a-zA-Z0-9]")
	if err != nil {
		log.Fatal(err)
	}
	words := strings.Fields(strings.ToLower(reg.ReplaceAllString(string(input), " ")))
	return words
}

func main() {
	var files []string
	files = append(files, "raven.txt")
	divide2(files)
}
