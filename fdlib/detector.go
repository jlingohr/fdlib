package fdlib

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	logger *log.Logger = log.New(os.Stdout, "FDLib-Detector", log.Lshortfile)
)

func checkError(err error) {
	if err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}
}


type Detector struct {
	epochNonce   uint64
	notifyChan   chan FailureDetected
	respondingChan chan struct{}
	monitoring   map[string]Monitor // Map from remote IP:PORT to Monitor
	seqNum
}

func CreateDetector(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	notifyChan := make(chan FailureDetected, ChCapacity)
	respondingChan := make(chan struct{})
	monitoring := make(map[string]Monitor)
	seqNum := newSeqNum()

	fd = Detector{EpochNonce, notifyChan, respondingChan, monitoring, seqNum}
	//TODO return error if called multiple times with the same EpochNonce

	return
}

type Monitor struct {
	Address   string
	Threshold uint8
	done      chan struct{}
}

func newMonitor(address string, threshold uint8) Monitor {
	stopChan := make(chan struct{})
	monitor := Monitor{address, threshold, stopChan}
	return monitor
}

type seqNum struct {
	SeqNum uint64 // TODO should be initialized randomly
	mux sync.Mutex
}

func newSeqNum() seqNum {
	//return seqNum{SeqNum: rand.Uint64()}
	return seqNum{SeqNum: 0}
}

// Tells the library to start responding to heartbeat messages on
// a local UDP IP:port. Can return an error that is related to the
// underlying UDP connection.
func (fd Detector) StartResponding(LocalIpPort string) (err error) {
	logger.Println(fmt.Sprintf("StartResponding - Responding to heartbeats on [%s]", LocalIpPort))
	lAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
	checkError(err)
	conn, err := net.ListenUDP("udp", lAddr)
	//defer conn.Close()
	checkError(err)

	bufIn := make([]byte, 1024)
	go func() {
		for {
			select {
			case <- fd.respondingChan:
				logger.Println(fmt.Sprintf("StartResponding - No longer responding on [%s]", LocalIpPort))
				break
			default:
				n, rAddr, err := conn.ReadFromUDP(bufIn)
				checkError(err)
				var hbeatMsg HBeatMessage
				err = json.Unmarshal(bufIn[:n], &hbeatMsg)
				checkError(err)
				logger.Println(fmt.Sprintf("StartResponding - Received heartbeat [%s]", string(bufIn)))

				ackMsg := AckMessage{hbeatMsg.EpochNonce, hbeatMsg.SeqNum}
				bufOut, err := json.Marshal(ackMsg)
				logger.Println(fmt.Sprintf("StartResponding - Sending ack [%s] to [%s]", string(bufOut), rAddr))
				conn.WriteToUDP(bufOut, rAddr)
			}

		}
	}()

	return
}

// Tells the library to stop responding to heartbeat
// messages. Always succeeds.
func (fd Detector) StopResponding() {
	logger.Println("StopResponding - No longer responding to heartbeats")
	close(fd.respondingChan)
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
		// Create new monitor
		logger.Println("Adding supervisee to monitor")
		monitor = newMonitor(RemoteIpPort, LostMsgThresh)
		fd.monitoring[RemoteIpPort] = monitor
	}
	// Update threshold if different
	if monitor.Threshold != LostMsgThresh {
		logger.Println("Updating lost message threshold")
		monitor.Threshold = LostMsgThresh
	}

	fd.monitor(LocalIpPort, monitor)

	return
}

// Tells the library to stop monitoring a particular remote UDP
// IP:port. Always succeeds.
func (fd Detector) RemoveMonitor(RemoteIpPort string) {
	monitored, contains := fd.monitoring[RemoteIpPort]
	if contains {
		logger.Println(fmt.Sprintf("RemoveMonitor - Removing [%s]", RemoteIpPort))
		close(monitored.done)
		delete(fd.monitoring, RemoteIpPort)
	}
	return
}

// Tells the library to stop monitoring all nodes.
func (fd Detector) StopMonitoring() {
	logger.Println("StopMonitoring - Stopping all heartbeats...")
	for _, monitor := range(fd.monitoring) {
		fd.RemoveMonitor(monitor.Address)
	}
}

//////////////////
// Private methods

func (fd Detector) monitor(LocalIpPort string, s Monitor) {
	logger.Println("monitor - Setting up heartbeat")

	go func() {
		lAddr, err := net.ResolveUDPAddr("udp", LocalIpPort)
		checkError(err)
		rAddr, err := net.ResolveUDPAddr("udp", s.Address)
		conn, err := net.DialUDP("udp", lAddr, rAddr)
		checkError(err)
		//defer conn.Close()

		failure := make(chan struct{}) // Channel to signal failure

		select {
		case <-s.done:
			logger.Println("monitor - Stopping heartbeat")
			return
		case <- failure:
			logger.Println("monitor - Failed to get any acks. Shutting down monitor")
			fd.notifyFailureDetected(s.Address)
			fd.RemoveMonitor(s.Address)
			return
		default:
			fd.heartbeat(conn, s, failure)
		}
	}()
	return
}

func (fd Detector) getSeqNum() uint64 {
	fd.mux.Lock()
	defer fd.mux.Unlock()
	fd.seqNum.SeqNum += 1
	return fd.seqNum.SeqNum
}

func (fd Detector) notifyFailureDetected(remoteAddr string) {
	currentTime := time.Now()
	failureDetectedMsg := FailureDetected{remoteAddr, currentTime}
	fd.notifyChan <- failureDetectedMsg
}

func (fd Detector) heartbeat(conn *net.UDPConn, m Monitor, failure chan struct{}) (err error) {
	attempt := uint8(0)
	success := make(chan struct{})
	for attempt < m.Threshold {
		// Make request
		seqNum := fd.getSeqNum()
		hbeatMsg := HBeatMessage{fd.epochNonce, seqNum}

		go fd.makeRequest(conn, hbeatMsg, success)

		select {
		case <- time.After(3 * time.Second): //TODO
			attempt += 1
		case <- success:
			attempt = 0
		}
	}
	logger.Println("monitor - Failed to get any acks. Shutting down monitor")
	failure <- struct{}{}
	return
}

func (fd Detector) makeRequest(conn *net.UDPConn, msg HBeatMessage, success chan struct{}) {
	bufIn := make([]byte, 1024)
	bufOut, err := json.Marshal(msg)
	checkError(err)
	logger.Println(fmt.Sprintf(
		"makeRequest - Sending new heartbeat request [%s]",
		string(bufOut)))

	_, err = conn.Write(bufOut)

	n, err := conn.Read(bufIn)
	checkError(err)

	if n > 0 {
		var ackMsg AckMessage
		err = json.Unmarshal(bufIn[:n], &ackMsg)
		checkError(err)
		logger.Println(fmt.Sprintf("makeRequest - Received heartbeat [%s]", string(bufIn[:n])))

		// Check ack is for the correct heartbeat
		if ackMsg.HBEatEpochNonce == fd.epochNonce && ackMsg.HBEatSeqNum == msg.SeqNum {
			logger.Println("makeRequest - Success! Received matching ack")
			logger.Println("makeRequest - Resetting attempts and deadline")
			success <- struct{}{}
			return
		}
	}

	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		logger.Println("monitor - Failed to get Ack")
		return
	}
}
