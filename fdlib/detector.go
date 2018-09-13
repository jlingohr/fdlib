package fdlib

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
)

var (
	logger *log.Logger = log.New(os.Stdout, "FDLib-", log.Lshortfile)
)

func checkError(err error) {
	if err != nil {
		logger.Fatal(err.Error())
		//os.Exit(1)
	}
}


type Detector struct {
	epochNonce   uint64
	notifyChan   chan FailureDetected
	monitoring   map[string]*Monitor // Map from remote IP:PORT to Monitor
	respondingChan chan struct{} // Channel the local listens to
	//seqNum
}

/////////////
// fdlib instance constructor

func CreateDetector(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	notifyChan := make(chan FailureDetected, ChCapacity)
	respondingChan := make(chan struct{})
	monitoring := make(map[string]*Monitor)
	//seqNum := newSeqNum()

	fd = Detector{EpochNonce, notifyChan, monitoring, respondingChan}
	//TODO return error if called multiple times with the same epochNonce

	return
}

/////////////
// fdlib implementation

// Tells the library to start responding to heartbeat messages on
// a local UDP IP:port. Can return an error that is related to the
// underlying UDP connection.
func (fd Detector) StartResponding(LocalIpPort string) (err error) {
	lAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
	checkError(err)
	conn, err := net.ListenUDP("udp", lAddr)
	checkError(err)

	go func() {
		//defer conn.Close() //TODO
		bufIn := make([]byte, 1024)
		for {
			logger.Println(fmt.Sprintf("StartResponding - Waiting for heartbeats on [%s]", LocalIpPort))
			select {
			case <- fd.respondingChan:
				logger.Println(fmt.Sprintf("StartResponding - No longer responding on [%s]", LocalIpPort))
				return
			default:
				n, rAddr, err := conn.ReadFromUDP(bufIn)
				checkError(err)

				// Handle heartbeat
				var hbeatMsg HBeatMessage
				err = json.Unmarshal(bufIn[:n], &hbeatMsg)
				checkError(err)
				logger.Println(fmt.Sprintf("StartResponding - Received heartbeat [%s] on [%s] from [%s]",
					string(bufIn[:n]), LocalIpPort, rAddr.String()))

				// Respond with Ack
				ackMsg := AckMessage{hbeatMsg.EpochNonce, hbeatMsg.SeqNum}
				bufOut, err := json.Marshal(ackMsg)
				_, err = conn.WriteToUDP(bufOut, rAddr)
				checkError(err)
			}
		}
	}()

	return
}

// Tells the library to stop responding to heartbeat
// messages. Always succeeds.
func (fd Detector) StopResponding() {
	logger.Println("StopResponding - No longer responding to heartbeats")
	fd.respondingChan <- struct{}{}
	return
}


// Tells the library to start monitoring a particular UDP IP:port
// with a specific lost messages threshold. Can return an error
// that is related to the underlying UDP connection.
func (fd Detector) AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error) {
	logger.Println(fmt.Sprintf(
		"AddMonitor - LocalIpPort [%s], RemoteIpPort [%s], LostMsgThres [%d]",
		LocalIpPort,
		RemoteIpPort,
		LostMsgThresh))

	monitor, contains := fd.monitoring[RemoteIpPort]
	if !contains {
		// Create new startMonitor
		logger.Println("AddMonitor - Adding supervisee to startMonitor")
		monitor, err = NewMonitor(LocalIpPort, RemoteIpPort, LostMsgThresh, fd.epochNonce)
		checkError(err)
		fd.monitoring[RemoteIpPort] = monitor
	}
	// Update threshold if different
	if monitor.Threshold != LostMsgThresh {
		logger.Println("AddMonitor - Updating lost message threshold")
		monitor.Threshold = LostMsgThresh
	}

	//go StartMonitor(monitor, fd.notifyChan)

	fd.startMonitor(monitor)

	return
}

// Tells the library to stop monitoring a particular remote UDP
// IP:port. Always succeeds.
func (fd Detector) RemoveMonitor(RemoteIpPort string) {
	monitored, contains := fd.monitoring[RemoteIpPort]
	if contains {
		logger.Println(fmt.Sprintf("RemoveMonitor - Removing [%s]", RemoteIpPort))
		monitored.killSwitch <- struct{}{}
	}
	return
}

// Tells the library to stop monitoring all nodes.
func (fd Detector) StopMonitoring() {
	logger.Println("StopMonitoring - Stopping all heartbeats...")
	for _, monitor := range(fd.monitoring) {
		fd.RemoveMonitor(monitor.RemoteAddress.String())
	}
}

//////////////////
// Private methods

func (fd Detector) startMonitor(monitor *Monitor) {

	go func() {
		failureChan := make(chan FailureDetected)
		go StartMonitor(monitor, fd.notifyChan, failureChan)
		detected := <- failureChan
		fd.RemoveMonitor(detected.UDPIpPort)
		return
	}()
}






