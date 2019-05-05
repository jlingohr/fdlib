package main


import (
	"../fdlib"
)

import "fmt"
import "time"
import "./common"


// Start and Stop monitoring
func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8080"
	var lostMsgThresh uint8 = 5

	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5

	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if common.CheckError(err) != nil {
		return
	}

	err = fd.StartResponding(localIpPort)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started responding to heartbeats.")

	// Add a monitor for a remote node.
	localIpPortMon := "127.0.0.1:9001"
	err = fd.AddMonitor(localIpPortMon, localIpPort, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", localIpPort)

	stopResponding := time.NewTicker(5*time.Second)
	done := time.NewTicker(45*time.Second)
	notfications := 0
	for {
		select {
		case <- stopResponding.C:
			stopResponding.Stop()
			fd.StopResponding()
		case <- notifyCh:
			notfications += 1
		case <- done.C:
			done.Stop()
			if notfications == 1 {
				fmt.Println("Success")
			}
			return
		}
	}

}

