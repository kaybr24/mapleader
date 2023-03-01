package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
)

var count = 0

// Keeps track of where a chunk of words have been processed and a slice of the words themselves
type ChunkInfo struct {
	processed bool     //Has this chunk been processed?
	words     []string //words to process
}

// each client served by a separate thread that executes this handleConnection function
// while one client is served, the server able to interact with other clients
func handleConnection(c net.Conn, data [10]ChunkInfo) {
	fmt.Print(".")
	for {
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		temp := strings.TrimSpace(string(netData)) //cuts leading and trailing spaces
		temp = strings.ToUpper(temp)
		if temp == "stop" {
			break
		} else if temp == "ready" {
			//check to see if any files need processing
			for _, chunk := range data { //an ALL DONE flag would make this more efficient
				if !chunk.processed {
					//if there is an unprocessed chunk, send the keyword "map" to the worker
					fmt.Println("recieved " + temp)
					c.Write([]byte("map" + "\n"))
					break //we only want to give this worker 1 chunk
				}
			}
		} else if temp == "ok map" {
			fmt.Println("recieved " + temp)
			allChunksProcessed := true
			//check to see if any files STILL need processing
			for _, chunk := range data {
				if !chunk.processed {
					allChunksProcessed = false
					//convert []string to string
					var data string = ""
					for _, word := range chunk.words {
						data += word
					}
					//send the chunk to the worker
					c.Write([]byte(data)) // can we have a shared file system?
					break                 //we only want to give this worker 1 chunk
				}
			}
			if allChunksProcessed {
				c.Write([]byte("done")) //Tell the worker that there are no more chunks to be processed
			}
		} else {
			fmt.Println("Unexpected command: " + temp)
		}
	}
	c.Close()
}

// getfiles takes as input a path to a directory in the form of a string and
// returns the names of the files within that directory in an array of strings.
func getFiles(dir string) []string {
	paths, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	files := make([]string, len(paths))
	for i, file := range paths {
		files[i] = dir + "/" + file.Name()
		if file.IsDir() {
			log.Fatal("Failure reading files. At least one of the directory "+
				"contents is a subdirectory. ", err)
		}
	}
	return files
}

// Divides one input file into 10 chunks
// returns an array of 10 pointers
// each pointer indicates a ChunkInfo tuples containing a bool processed and []string list of words
func divide(files []string) [10]ChunkInfo {
	var chunks [10]ChunkInfo     //array of slices
	words := readFile(files[0])  //*assuming there is only one file in files* Get a list of all the words in the file
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
	return chunks
}

// Given a file name, opens that file and returns an array of bytes.
// Logs if there is an array opening and reading the file.
func readFile(filename string) []string {
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
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a port number!")
		return
	}
	if len(arguments) == 2 {
		fmt.Println("Please provide an input directory name!")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	inputDirectory := arguments[2] //directory where input is stored
	fmt.Println(inputDirectory)
	files := getFiles(inputDirectory) //list of names of files in inputDirectory, assuming there are no subfolders
	//fmt.Println(files)
	fileChunks := divide(files)
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c, fileChunks)
		count++ //counter never decrements if a client leaves?
	}
}
