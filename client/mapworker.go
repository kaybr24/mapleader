package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// Type using Mutex to guarantee mutual exclusion and safe access in
// critical section, since freqMap will be shared among threads.
type SafeMap struct {
	freqMap map[string]int
	m       *sync.RWMutex
}

// Given a file name and a SafeMap, reads the file, counts the words in it,
// and maps the counts to the words in freq.freqMap.
func multiCountFile(file string, freq SafeMap) {
	//split string into separate words
	reg, err := regexp.Compile("[^a-zA-Z0-9]")
	if err != nil {
		log.Fatal(err)
	}
	//words is a []string of indivdiual words
	words := strings.Fields(strings.ToLower(reg.ReplaceAllString(string(file), " ")))
	fmt.Println(words)

	//make a map of wordcounts for words in file
	for _, w := range words {
		freq.m.Lock()
		freq.freqMap[w]++
		freq.m.Unlock()
	}
}

/*
// Function that creates a thread for each file in the given directory.
//Given a list of file names as strings, returns the wordcounts of each word in the files
func multi_threaded_file(files []string) SafeMap {
	freq := SafeMap{freqMap: make(map[string]int), m: &sync.RWMutex{}}
	wg := sync.WaitGroup{}

	for i := 0; i < len(files); i++ {
		wg.Add(1)
		go func(idx int, freq SafeMap, wg *sync.WaitGroup) {
			defer wg.Done()
			multiCountFile(files[idx], freq)
		}(i, freq, &wg)
	}

	wg.Wait()
	return freq
}
*/

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide host:port.")
		return
	}

	CONNECT := arguments[1]
	c, err := net.Dial("tcp", CONNECT) //c is a channel, err is the error message (indicates c was not created right)
	if err != nil {
		fmt.Println(err)
		return
	}

	currentlyWorking := false

	counter := 0
	//I think to change this, rather than getting input from user keyboard, needs to be getting input from the channel exclusively?
	//
	//if leader gets 2 worker responses at same time, want to make sure you arent modifying dictionary at exact same time from diff threads --> need to do mutual exclusion
	for {
		//I'm not sure we need this entire chunk?
		/*reader := bufio.NewReader(os.Stdin) //sets up standard input reader
		fmt.Print(">> ")
		text, _ := reader.ReadString('\n') //read the input from os.Stdin, which the user typed input
		fmt.Fprintf(c, text+"\n")          //formatted print, which basically writes the input to the channel
		*/
		fmt.Println("NEW CLIENT FOR LOOP ITERATION")
		counter += 1

		if !currentlyWorking {
			fmt.Fprintf(c, "ready\n")
			currentlyWorking = true
		}
		message, _ := bufio.NewReader(c).ReadString('\n') // bufio reads from the channel (c) now, reading the string that ends with and 'enter'
		if err != nil {
			fmt.Println(err)
			return
		}
		message = message[:len(message)-1]
		message = strings.ToLower(message)

		if message == "map" {
			fmt.Print("did I get map? " + message)
			//fmt.Fprintf(c, "ok map\n")
			//c.Write([]byte("ok map\n"))
			numBytes, err := c.Write([]byte("ok map" + "\n"))
			if err != nil {
				fmt.Println(err)
				return
			} else {
				fmt.Println("Printed: " + strconv.Itoa(numBytes))
			}
			fmt.Println("I JUST SENT ok map")

		} else if message == "done" {
			fmt.Print("am I done? " + message)
			fmt.Println("TCP client exiting...")
			return
		} else { //otherwise, we assume that message is a chunk*

			//convert large string into word counts
			data := SafeMap{freqMap: make(map[string]int), m: &sync.RWMutex{}}
			multiCountFile(message, data)
			fmt.Print("->: ")
			fmt.Print(data.freqMap)

			//send that information over the channel to the server line by line
			for word, count := range data.freqMap {
				fmt.Fprintf(c, "%s; %v\n", word, count) //";" is not a word character
			}

		}

		//if message is "map" from the channel, respond back with "ok map"
		//right after it sends "ok map", should receive data and then start processing right away
		//before the processing has started, make a bool that is ready = true. once entered the processing part of the code, change this to false.
		//after the data has been sent back to the server, change the bool back to true
		//does wordcount, then sends back the intermediate data (in a map, I hope?) how to send a map in bytes?
		//if message is "done", then stop the process by doing a return
		/*message, _ := bufio.NewReader(c).ReadString('\n') // bufio reads from the channel (c) now, reading the string that ends with and 'enter'
		fmt.Print("->: " + message)                       // this is a normal print out
		if strings.TrimSpace(string(text)) == "STOP" {    // text is the text read from the std input
			fmt.Println("TCP client exiting...")
			return
		}*/
		//when sending it back, could send back as a file, or turn it into a string and then turn it back etc
		//if STOP was not entered, it will keep looping (endlessly, until STOP is entered)
	}
}
