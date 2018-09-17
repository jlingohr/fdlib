/*

This package specifies the API to the failure detector library to be
used in assignment 1 of UBC CS 416 2018W1.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Initialize, but you
cannot change its API.

*/

package fdlib

import (
	"fmt"
	"time"
)

//////////////////////////////////////////////////////
// Define the message types fdlib has to use to communicate to other
// fdlib instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fdlib instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

//////////////////////////////////////////////////////

// An FD interface represents an instance of the fd
// library. Interfaces are everywhere in Go:
// https://gobyexample.com/interfaces
type FD interface {
	// Tells the library to start responding to heartbeat messages on
	// a local UDP IP:port. Can return an error that is related to the
	// underlying UDP connection.
	StartResponding(LocalIpPort string) (err error)

	// Tells the library to stop responding to heartbeat
	// messages. Always succeeds.
	StopResponding()

	// Tells the library to start monitors a particular UDP IP:port
	// with a specific lost messages threshold. Can return an error
	// that is related to the underlying UDP connection.
	AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error)

	// Tells the library to stop monitors a particular remote UDP
	// IP:port. Always succeeds.
	RemoveMonitor(RemoteIpPort string)

	// Tells the library to stop monitors all nodes.
	StopMonitoring()
}

// The constructor for a new FD object instance. Note that notifyCh
// can only be received on by the client that receives it from
// initialize:
// https://www.golang-book.com/books/intro/10
func Initialize(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	// TODO
	//return nil, nil, errors.New("unimplemented")
	if FDLib == nil {
		return CreateDetector(EpochNonce, ChCapacity)
	} else {
		FDLib.Stop()
		FDLib = nil
		err = InitializeError("Multiple calls to Initialize not allowed.")
		return nil, nil, err
	}

}

type InitializeError string

func (e InitializeError) Error() string {
	return fmt.Sprintf("[%s]", e.Error())
}