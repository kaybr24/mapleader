package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var count = 0

// track the index of the data chunk that needs to be processed next
// Assuming there are no faults, this is also the number of data chunks which have already been processed
type safeIndex struct {
	index int
	m     *sync.RWMutex
}

var lastIndexProcessed = safeIndex{index: -1, m: &sync.RWMutex{}}

//var lastIndexProcessed = -1

/*
// Keeps track of where a chunk of words have been processed and a slice of the words themselves
type ChunkInfo struct {
	processed bool     //Has this chunk been processed?
	words     []string //words to process
}
*/

// given the whole divided input data, return the next tenth as a string
// if all data has been processed, returns ""
func getData(ALLdata [10][]string) string {
	//MUTEX
	lastIndexProcessed.m.Lock()
	lastIndexProcessed.index += 1 //assume that the data will be processed perfectly from here on out
	i := lastIndexProcessed.index
	lastIndexProcessed.m.Unlock()
	//MUTEX
	var chunk string
	if i < 10 {
		chunk = sliceToString(ALLdata[i])
	}
	return chunk
}

// each client served by a separate thread that executes this handleConnection function
// while one client is served, the server able to interact with other clients
func handleConnection(c net.Conn, ALLdata [10][]string, collect map[string]int) { //add wg *sync.WaitGroup
	fmt.Print(".")
	for {
		fmt.Println("NEW SERVER FOR LOOP ITERATION")
		var workToDo bool
		var currInd int
		lastIndexProcessed.m.Lock()
		workToDo = lastIndexProcessed.index < 10
		currInd = lastIndexProcessed.index
		lastIndexProcessed.m.Unlock()
		fmt.Printf("Do we have work to do?: %t, it is chunk #%v\n", workToDo, currInd)

		//read in data from the channel
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Print("netData is: " + netData)

		temp := strings.TrimSpace(string(netData)) //cuts leading and trailing spaces
		temp = strings.ToLower(temp)
		//fmt.Println("Temp is: " + temp)
		if temp == "stop" {
			break
		} else if temp == "ready" {
			if workToDo { //true if any files need processing
				//if there is an unprocessed chunk, send the keyword "map" to the worker
				fmt.Println("recieved " + temp)
				c.Write([]byte("map" + "\n"))
				//break
			} else {
				fmt.Println("SENT DONE after worker said ready")
				c.Write([]byte("done" + "\n")) //Tell the worker that there are no more chunks to be processed
				//break
			}
		} else if temp == "ok map" {
			if workToDo {
				fmt.Println("recieved " + temp)
				//send the chunk to the worker
				data := getData(ALLdata)
				c.Write([]byte(data + "\n")) // can we have a shared file system?
			} else {
				fmt.Println("SENT DONE after worker said ok map")
				c.Write([]byte("done" + "\n")) //Tell the worker that there are no more chunks to be processed
				//break
			}
		} else if temp[0] == '(' { //assume we are recieving data from a mapper
			collectWorkerOutput(&temp, &collect) //uses pointers so we don't create copies of temp and collect
			//continue
		} else if temp == "finished!" { //the worker tells us they are done
			fmt.Println("SENT NEW MAP after worker said finished!")
			c.Write([]byte("map" + "\n")) //Ask the worker if they can do more mapping
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
// returns an array of 10 pointers to sgements of data.  Chunks are stored as type string
// will need to lock the chunk so that other workers dont access it
func divide(files []string) [10][]string {
	var chunks [10][]string      //array of string slices
	words := readFile(files[0])  //*assuming there is only one file in files* Get a list of all the words in the file
	totalWords := len(words)     //total number of words in input
	chunkSize := totalWords / 10 //expected number of words per chunk
	//*The last mapper may have nearly twice as much work as other mappers (uneven split)
	for i := 0; i < 9; i++ { //loop 10 times
		if len(words[i*chunkSize]) > 0 { //just in case we have a very small input file
			info := words[i*chunkSize : (i+1)*chunkSize]
			chunks[i] = info
		}

	}
	//for the remainder
	info := words[9*chunkSize:]
	chunks[9] = info
	//now we have 10 chunks of words
	return chunks
}

/*
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
*/

/*
// Check if there are chunks left to process
// If there are, return bool false and 1 file chunk
// If everything is processed, return bool true and empty string
func isProcessingDone(data [10]ChunkInfo) (bool, string) {
	//check to see if any files STILL need processing
	for _, chunk := range data {
		if !chunk.processed { //if there is an unprocessed chunk
			//convert []string to string
			var chunkToSend string = ""
			for _, word := range chunk.words {
				chunkToSend += word + " "
			}
			//send the chunk to the worker
			//*I think the lock needs to happen here? and unlock once the data is sent back
			return false, chunkToSend
		}
	}
	return true, ""
}
*/

// takes []string of words with no spaces
// returns same data as type string (words are separated by spaces)
// enables sending data over the channel
func sliceToString(dataSlice []string) string {
	var dataString string
	for _, word := range dataSlice {
		dataString += word + " " //append the next word, separated by spaces
	}
	return dataString
}

// update the output map (collect) with a new word count from a worker
func collectWorkerOutput(tempP *string, collectP *map[string]int) {
	temp := *tempP
	collect := *collectP                                          //get rid of '('
	word := temp[1:strings.IndexByte(temp, ';')]                  //the word is before ';'
	count, _ := strconv.Atoi(temp[strings.IndexByte(temp, ';'):]) //the word count is after ';'
	//add the count
	collect[word] += count
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

// Takes as input a hashmap with string keys and integer values and creates,
// sorts, and returns an array of the hashmap's keys.
func sortWords(freq map[string]int) []string {
	words := make([]string, len(freq))
	i := 0
	for key := range freq {
		words[i] = key
		i += 1
	}
	sort.Strings(words)
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
	fileChunks := divide(files)            //returns [10][]string
	dataCollection := make(map[string]int) //Our collector for all worker data
	//wg := sync.WaitGroup{}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		//create a new thread to handle that client
		go handleConnection(c, fileChunks, dataCollection)

		//check if we need the worker to process data
		//idea: all code in handleConnection, keep a global? var that is last index checked -> start val of -1
		//every time need to send a chunk, increment by 1, and then use that nest chunk at that index as the chunk t be sent
		//get rid of continues, get rid of breaks from server
		/*
			done, fileChunk := isProcessingDone(fileChunks)
			if done {
				//tell the worker "done"
				fmt.Print("Notifying mapper that all work is done!")
				go handleConnection(c, fileChunk, dataCollection, done)
				//write output?
			} else {
				go handleConnection(c, fileChunk, dataCollection, !done) //version without waitgroup
			}
		*/
		//wg.Add(1)
		//go handleConnection(c, fileChunks, dataCollection, wg*sync.WaitGroup)

		//defer wg.Done()
		//need to make sure the filed can be broken up, need to make it multithreaded
		//similar to dictionary in assign1
		count++ //counter never decrements if a client leaves?

		//If all chunks have ben processed, print the results
		lastIndexProcessed.m.Lock()
		currentChunk := lastIndexProcessed.index
		lastIndexProcessed.m.Unlock()

		if currentChunk >= 10 {
			fmt.Println("I am getting to a count of 10")
			writeToFile(sortWords(dataCollection), "output/results.txt", dataCollection)
		}
	}
	//wg.Wait()

}
