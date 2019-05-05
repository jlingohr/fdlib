
package main


import "../fdlib"

import "fmt"
import "time"
import "./common"

// Run a client. Assumes another node is already setup
func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	toMonitorIpPortA := "127.0.0.1:9000"
	var lostMsgThresh uint8 = 5

	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5

	// Initialize fdlib. Note the use of multiple assignment:
	// https://gobyexample.com/multiple-return-values
	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if common.CheckError(err) != nil {
		return
	}

	defer fd.StopMonitoring()


	// Add a monitor for a remote node.
	localIpPortMonA := "127.0.0.1:8080"
	localIpPortMonB := "127.0.0.1:8081"
	localIpPortMonC := "127.0.0.1:8082"
	localIpPortMonD := "127.0.0.1:8083"
	localIpPortMonE := "127.0.0.1:8084"
	err = fd.AddMonitor(localIpPortMonA, toMonitorIpPortA, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node on [%s]: ", localIpPortMonA)

	err = fd.AddMonitor(localIpPortMonB, toMonitorIpPortA, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node on [%s]: ", localIpPortMonB)

	err = fd.AddMonitor(localIpPortMonC, toMonitorIpPortA, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node on [%s]: ", localIpPortMonC)

	err = fd.AddMonitor(localIpPortMonD, toMonitorIpPortA, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node on [%s]: ", localIpPortMonD)

	err = fd.AddMonitor(localIpPortMonE, toMonitorIpPortA, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node on [%s]: ", localIpPortMonE)


	done := time.NewTicker(60*time.Second)
	for {
		select {
		case notify := <-notifyCh:
			fmt.Println("Failed - Detected a failure of", notify)
			return

		case <- done.C:
			done.Stop()
			fmt.Println("Success")
			return
		}
	}

}

