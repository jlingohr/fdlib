/*

A trivial application to illustrate how the fdlib library can be used
in assignment 1 for UBC CS 416 2018W1.

Usage:
go run client.go
*/

package main

// Expects fdlib.go to be in the ./fdlib/ dir, relative to
// this client.go file
import (
	"../fdlib"
)

import "fmt"
import "./common"


// Initialize twice should give an error
func main() {

	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5

	_, _, err := fdlib.Initialize(epochNonce, chCapacity)
	if common.CheckError(err) != nil {
		return
	}

	fd, notifyCh, err := fdlib.Initialize(epochNonce+1, chCapacity)
	if common.CheckError(err) != nil {
		if fd == nil && notifyCh == nil {
			fmt.Println("Success")
		} else {
			fmt.Println("Failre")
		}
	}

}

