package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

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
		counter += 1

		if !currentlyWorking {
			fmt.Fprintf(c, "ready\n")
			currentlyWorking = true
		}
		message, _ := bufio.NewReader(c).ReadString('\n') // bufio reads from the channel (c) now, reading the string that ends with and 'enter'
		message = strings.ToLower(message)

		if message == "map" {
			fmt.Print("->: " + message)
			//fmt.Fprintf(c, "ok map\n")
			c.Write([]byte("ok map\n"))
			/*gitnumBytes, err := fmt.Fprintf(c, "ok map\n")
			if err != nil {
				fmt.Println(err)
				return
			} else {
				fmt.Println("Printed: " + strconv.Itoa(numBytes))
			}*/
			fmt.Println("I JUST SENT ok map")

		} else if message == "done" {
			fmt.Print("->: " + message)
			fmt.Println("TCP client exiting...")
			return
		} else { //otherwise, we assume that message is a chunk*
			if counter > 100 {
				break
			} else {
				fmt.Print("->: " + message)
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
