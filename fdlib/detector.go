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
	FDLib *Detector
)

func checkError(err error) bool {
	if err != nil {
		logger.Println(err.Error())
		return true
	}
	return false
}


type Detector struct {
	epochNonce   uint64
	notifyChan   chan FailureDetected
	monitoring   map[string]*Monitor // Map from remote IP:PORT to Monitor
	respondingChan chan struct{} // Channel the local listens to
	remoteToIDMap map[string]string // Map from remote address to Local::Remote id
	//seqNum
}

/////////////
// fdlib instance constructor

func CreateDetector(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	notifyChan := make(chan FailureDetected, ChCapacity)
	respondingChan := make(chan struct{})
	monitoring := make(map[string]*Monitor)
	idMap := make(map[string]string)
	//seqNum := newSeqNum()

	fd = Detector{EpochNonce, notifyChan, monitoring, respondingChan, idMap}
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

	id := LocalIpPort + "::" + RemoteIpPort

	monitor, contains := fd.monitoring[id]
	if !contains {
		// Create new startMonitor
		logger.Println("AddMonitor - Adding supervisee to startMonitor")
		monitor, err = NewMonitor(LocalIpPort, RemoteIpPort, LostMsgThresh, fd.epochNonce)
		if err == nil {
			fd.remoteToIDMap[RemoteIpPort] = id
			fd.monitoring[id] = monitor
			fd.startMonitor(monitor)
		}
		return
	}
	if monitor.Threshold != LostMsgThresh {
		logger.Println("AddMonitor - Updating lost message threshold")
		monitor.killSwitch <- struct{}{}
		monitor.Threshold = LostMsgThresh
		fd.startMonitor(monitor)
		return
	}
	return
}

// Tells the library to stop monitoring a particular remote UDP
// IP:port. Always succeeds.
func (fd Detector) RemoveMonitor(RemoteIpPort string) {
	id, contains := fd.remoteToIDMap[RemoteIpPort]
	if !contains {
		return
	}
	monitored, contains := fd.monitoring[id]
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
		fd.RemoveMonitor(monitor.Conn.RemoteAddr().String())
	}
}

func (fd Detector) Stop() {
	return
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






