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

// Type using Mutex to guarantee mutual exclusion and safe access in
// critical section, since dataCollection will be shared among threads.
type SafeMap struct {
	countsMap map[string]int
	m         *sync.RWMutex
}

// track the index of the data chunk that needs to be processed next
// Assuming there are no faults, this is also the number of data chunks which have already been processed
type SafeIndex struct {
	index int
	m     *sync.RWMutex
}

var lastIndexProcessed = SafeIndex{index: -1, m: &sync.RWMutex{}}

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
func handleConnection(c net.Conn, ALLdata [10][]string, collectP *SafeMap) { //add wg *sync.WaitGroup
	fmt.Print(".")
	collect := *collectP
	numDONE := 0 //tracks the number of clients that have finished processing data
	for {
		fmt.Println("NEW SERVER FOR LOOP ITERATION")
		var workToDo bool
		var currInd int
		lastIndexProcessed.m.Lock()
		workToDo = lastIndexProcessed.index < 9
		currInd = lastIndexProcessed.index
		lastIndexProcessed.m.Unlock()
		fmt.Printf("Do we have work to do?: %t, it is chunk #%v\n", workToDo, currInd)

		//read in data from the channel
		netData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(netData) >= 15 {
			fmt.Println("netData is: " + netData[:15] + "...")
		} else {
			fmt.Println("netData is: " + netData)
		}

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
				if numDONE == 10 {             //If data has been recieved from all ten chunks:
					fmt.Println("I am getting to a processed chunk count of 10")
					writeToFile(sortWords(collect.countsMap), "output/results.txt", collect.countsMap)
				}
				//}
				break
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
				//if all chunks are processed, print results
				if numDONE == 10 {
					fmt.Println("I am getting to a processed chunk count of 10")
					writeToFile(sortWords(collect.countsMap), "output/results.txt", collect.countsMap)
				}
				break
			}
		} else if temp[0] == '(' { //assume we are recieving data from a mapper
			collectWorkerOutput_bymap(&temp, &collect) //uses pointers so we don't create copies of temp and collect
			numDONE += 1                               //Indicate that we have recieved results from another worker
		} else {
			fmt.Println("Unexpected command: " + temp)
		}
	}

	c.Close()
	fmt.Println("---> CLOSED CLIENT CONNECTION")
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

// convert a string into a list of words
// letters and numbers are seen as word characters; everything else is a spacer
func breakIntoWords(inputP *string) []string {
	input := *inputP
	reg, err := regexp.Compile("[^a-zA-Z0-9]")
	if err != nil {
		log.Fatal(err)
	}
	words := strings.Fields(strings.ToLower(reg.ReplaceAllString(string(input), " ")))
	return words
}

// update the output map (collect) with a new word count from a worker
// collects worker output as one giant map string
func collectWorkerOutput_bymap(tempP *string, collectP *SafeMap) {
	collect := *collectP //get rid of '('
	wordsList := breakIntoWords(tempP)
	//println(wordsList[:10])
	for i := 0; i < len(wordsList); i += 2 {
		word := wordsList[i]
		count, _ := strconv.Atoi(wordsList[i+1])
		//add the information
		collect.m.Lock()
		collect.countsMap[word] += count
		collect.m.Unlock()
	}

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
	fileChunks := divide(files)                                                    //returns [10][]string
	dataCollection := SafeMap{countsMap: make(map[string]int), m: &sync.RWMutex{}} //Our collector for all worker data
	//dataCollection := make(map[string]int) //Our collector for all worker data
	//wg := sync.WaitGroup{}

	for {
		//Look for clients that want to connect
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		//create a new thread to handle that client
		go handleConnection(c, fileChunks, &dataCollection)
	}
}
