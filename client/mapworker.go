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
	//Did we get some words?
	arbitraryNumber := 10
	if len(words) < 10 {
		arbitraryNumber = len(words)
	}
	fmt.Printf("RECIEVED FILE CHUNK: %q\n", words[0:arbitraryNumber])

	//make a map of wordcounts for words in file
	for _, w := range words {
		freq.m.Lock()
		freq.freqMap[w]++
		freq.m.Unlock()
	}
}

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
		if len(message) > 1 {
			message = message[:len(message)-1] //remove new line
			message = strings.ToLower(message)
		} else {
			fmt.Printf("-> I should NOT have recieved :%s", message)
		}

		if message == "map" {
			fmt.Println("did I get map? " + message)
			_, err := c.Write([]byte("ok map" + "\n"))
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("I JUST SENT ok map")

		} else if message == "done" {
			fmt.Println("am I done? " + message)
			fmt.Println("TCP client exiting...")
			return
		} else { //otherwise, we assume that message is a chunk*

			//convert large string into word counts
			data := SafeMap{freqMap: make(map[string]int), m: &sync.RWMutex{}}
			multiCountFile(message, data)
			fmt.Println("CREATED MAP OF WORDCOUNTS: ")
			//fmt.Print(data.freqMap)

			//send that information over the channel to the server as one giant string
			var mapToSend string
			for word, count := range data.freqMap {
				//fmt.Fprintf(c, "(%s;%v\n", word, count) //";" is not a word character
				mapToSend += "(" + word + ";" + strconv.Itoa(count)
			}
			fmt.Fprintf(c, mapToSend+"\n")

			fmt.Fprintf(c, "ready\n") //tell server that I am done processing this chunk
			currentlyWorking = false
			fmt.Println("FINISHED SENDING DATA CHUNK")

		}
	}
}
