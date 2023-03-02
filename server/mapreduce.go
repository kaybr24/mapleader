package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var count = 0

// Keeps track of where a chunk of words have been processed and a slice of the words themselves
type ChunkInfo struct {
	processed bool     //Has this chunk been processed?
	words     []string //words to process
}

// each client served by a separate thread that executes this handleConnection function
// while one client is served, the server able to interact with other clients
func handleConnection(c net.Conn, data [10]ChunkInfo, collect map[string]int, wg *sync.WaitGroup) {
	fmt.Print(".")
	for {
		fmt.Println("NEW FOR LOOP ITERATION")
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("netData is: " + netData)

		temp := strings.TrimSpace(string(netData)) //cuts leading and trailing spaces
		temp = strings.ToLower(temp)
		fmt.Println("Temp is: " + temp)
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
					allChunksProcessed = false //when do we update this to true? once the worker sends the data back? would need to use mutex somehow to lock this chunk? but is mutex done on the server or the client end?
					//convert []string to string
					var data string = ""
					for _, word := range chunk.words {
						data += word
					}
					//send the chunk to the worker
					//*I think the lock needs to happen here? and unlock once the data is sent back
					c.Write([]byte(data + "\n")) // can we have a shared file system?
					break                        // we only want to give this worker 1 chunk
				}
			}
			if allChunksProcessed {
				c.Write([]byte("done")) //Tell the worker that there are no more chunks to be processed
				break
			}
		} else if temp[0] == '(' { //assume we are recieving data from a mapper
			collectWorkerOutput(&temp, &collect) //uses pointers so we don't create copies of temp and collect
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
// will need to lock the chunk so that other workers dont access it
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

// update the output map (collect) with a new word count from a worker
func collectWorkerOutput(tempP *string, collectP *map[string]int) {
	temp := *tempP
	collect := *collectP
	temp = temp[1:]                                               //get rid of '('
	word := temp[:strings.IndexByte(temp, ';')]                   //the word is before ';'
	count, _ := strconv.Atoi(temp[strings.IndexByte(temp, ';'):]) //the word count is after ';'
	//add the count
	collect[word] += count //collect is initalized with 0 values so we don't need to check if word is in collect
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

// Given an array of words, a file name to write to, and count, a hashmap with
// string keys and integer values, creates a file with the provided name,
// writes in order the words in the array and the
// corresponding key value into the file.
func writeToFile(keys []string, filename string, count map[string]int) {
	// creating the output file
	output, err := os.Create(filename)
	if err != nil {
		log.Fatal("Error creating file: ", err)
	}

	defer output.Close()

	writer := bufio.NewWriter(output)
	for _, k := range keys {
		_, err := writer.WriteString(k + " " + strconv.Itoa(count[k]) + "\n")
		if err != nil {
			log.Fatal("Error writing to file: ", err)
		}
	}
	writer.Flush()
	output.Close()
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
	//fmt.Println(inputDirectory)
	files := getFiles(inputDirectory) //list of names of files in inputDirectory, assuming there are no subfolders
	//fmt.Println(files)
	fileChunks := divide(files)
	var dataCollection map[string]int //Our collector for all worker data
	wg := sync.WaitGroup{}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		wg.Add(1)
		go handleConnection(c, fileChunks, dataCollection, wg*sync.WaitGroup)
		defer wg.Done()
		//need to make sure the filed can be broken up, need to make it multithreaded
		//similar to dictionary in assign1
		count++ //counter never decrements if a client leaves?
	}
	wg.Wait()
	writeToFile([]string, "output/multi.txt", dataCollection)
}
