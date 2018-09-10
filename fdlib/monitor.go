package fdlib

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	seqNum seqNumber = newSeqNumber()
)

type seqNumber struct {
	SeqNum uint64 // TODO should be initialized randomly
	mux sync.Mutex
}

func newSeqNumber() seqNumber {
	//return seqNumber{SeqNum: rand.Uint64()}
	return seqNumber{SeqNum: 0}
}

func (s seqNumber) getSeqNum() uint64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	num := s.SeqNum
	s.SeqNum++
	return num
}

type Monitor struct {
	RemoteAddress string
	Threshold     uint8
	epochNonce    uint64
	killSwitch    chan struct{}
	delay         time.Duration
	Conn          *net.UDPConn
}

func newMonitor(localAddress string, remoteAddress string, threshold uint8, epochNonce uint64) (monitor Monitor, err error) {
	lAddr, err := net.ResolveUDPAddr("udp", localAddress)
	checkError(err) //TODO probably should just exit here
	rAddr, err := net.ResolveUDPAddr("udp", remoteAddress)
	// Connect to the remote node
	conn, err := net.DialUDP("udp", lAddr, rAddr)
	checkError(err)

	killSwitch := make(chan struct{})
	initialDelay := 3 * time.Second
	monitor = Monitor{remoteAddress, threshold, epochNonce ,killSwitch, initialDelay, conn}
	logger.Println(fmt.Sprintf("newMonitor - remote [%s], threshold [%d], nonce [%d]",
		monitor.RemoteAddress, monitor.Threshold, monitor.epochNonce))
	return
}

func (monitor Monitor) StartMonitor(notifyChan chan FailureDetected, shutdown chan struct{}) {

	select {
	case <- monitor.killSwitch:
		//TODO stop monitor
		logger.Println(fmt.Sprintf("StartMonitor - Killswitch hit. Shutting down monitor for remote [%s]", monitor.RemoteAddress))
		monitor.Conn.Close()
		close(monitor.killSwitch)
		return
	default:
		logger.Println(fmt.Sprintf("StartMonitor - Starting monitor for remote [%s]", monitor.RemoteAddress))
		monitor.heartbeat(notifyChan, shutdown)
	}

}

func (monitor Monitor) heartbeat(notifyChan chan FailureDetected, shutdown chan struct{}) (err error) {
	lostMessages := uint8(0)
	success := make(chan struct{})
	for lostMessages < monitor.Threshold {
		// Make request
		go monitor.makeRequest(success)

		select {
		case <- time.After(monitor.delay): //TODO
			lostMessages += 1
		case <- success:
			logger.Println("heartbeat - Resetting attempts and deadline")
			lostMessages = 0
		}
	}
	logger.Println(fmt.Sprintf("heartbeat - Failed to get any acks. Shutting down monitor for [%s]", monitor.RemoteAddress))
	notifyChan <- notifyFailureDetected(monitor.RemoteAddress)
	shutdown <- struct{}{}
	return
}

func (monitor Monitor) makeRequest(success chan struct{}) {
	seqNum := seqNum.getSeqNum()
	hbeatMsg := HBeatMessage{monitor.epochNonce, seqNum}

	bufOut, err := json.Marshal(hbeatMsg)
	checkError(err)
	logger.Println(fmt.Sprintf(
		"makeRequest - Sending new heartbeat request [%s] to remote [%s]",
		string(bufOut), monitor.RemoteAddress))

	//reqStartTime := time.Now() //TODO
	_, err = monitor.Conn.Write(bufOut)

	// Read ack
	bufIn := make([]byte, 1024)
	n, raddr, err := monitor.Conn.ReadFromUDP(bufIn)
	checkError(err)

	logger.Println(fmt.Sprintf("makeRequest - Read message [%s] from [%s]", string(bufIn[:n]), raddr.String()))

	if n > 0 {
		//reqEndTime := time.Now() //TODO
		var ackMsg AckMessage
		err = json.Unmarshal(bufIn[:n], &ackMsg)
		checkError(err)

		// TODO update RTT

		// Check ack is for the correct heartbeat
		if ackMsg.HBEatEpochNonce == hbeatMsg.EpochNonce && ackMsg.HBEatSeqNum == hbeatMsg.SeqNum {
			logger.Println("makeRequest - Success! Received matching ack")
			success <- struct{}{}
		} else {
			logger.Println(fmt.Sprintf("Expected nonce [%d] and seq [%d] but got nonce [%d] and seq [%d]",
				hbeatMsg.EpochNonce, hbeatMsg.SeqNum, ackMsg.HBEatEpochNonce, ackMsg.HBEatSeqNum))
		}
	}

	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		logger.Println("makeRequest - Failed to get Ack")
	}
}


func notifyFailureDetected(remoteAddr string) (failureDetectedMsg FailureDetected) {
	currentTime := time.Now()
	failureDetectedMsg = FailureDetected{remoteAddr, currentTime}
	return
}