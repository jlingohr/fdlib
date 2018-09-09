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
	s.SeqNum += 1
	return s.SeqNum
}

type Monitor struct {
	RemoteAddress string
	Threshold     uint8
	epochNonce    uint64
	killSwitch    chan struct{}
	delay         time.Duration
	Conn          *net.UDPConn
}

func newMonitor(localAddress string, remoteAddress string, threshold uint8, epochNonce uint64) Monitor {
	lAddr, err := net.ResolveUDPAddr("udp", localAddress)
	checkError(err) //TODO probably should just exit here
	rAddr, err := net.ResolveUDPAddr("udp", remoteAddress)
	conn, err := net.DialUDP("udp", lAddr, rAddr)
	checkError(err)

	killSwitch := make(chan struct{})
	initialDelay := 3 * time.Second
	monitor := Monitor{remoteAddress, threshold, epochNonce ,killSwitch, initialDelay, conn}
	return monitor
}

func (monitor Monitor) Start(failureChan chan struct{}) {

	select {
	case <- monitor.killSwitch:
		//TODO stop monitor
	default:
		monitor.heartbeat(failureChan)
	}

}

func (monitor Monitor) heartbeat(failure chan struct{}) (err error) {
	lostMessages := uint8(0)
	success := make(chan struct{})
	for lostMessages < monitor.Threshold {
		// Make request
		seqNum := seqNum.getSeqNum()
		hbeatMsg := HBeatMessage{monitor.epochNonce, seqNum}

		go monitor.makeRequest(hbeatMsg, success)

		select {
		case <- time.After(monitor.delay): //TODO
			lostMessages += 1
		case <- success:
			lostMessages = 0
		}
	}
	logger.Println("startMonitor - Failed to get any acks. Shutting down startMonitor")
	failure <- struct{}{}
	return
}

func (monitor Monitor) makeRequest(msg HBeatMessage, success chan struct{}) {
	bufIn := make([]byte, 1024)
	bufOut, err := json.Marshal(msg)
	checkError(err)
	logger.Println(fmt.Sprintf(
		"makeRequest - Sending new heartbeat request [%s]",
		string(bufOut)))

	//reqStartTime := time.Now() //TODO
	_, err = monitor.Conn.Write(bufOut)

	n, err := monitor.Conn.Read(bufIn)
	checkError(err)

	if n > 0 {
		//reqEndTime := time.Now() //TODO
		var ackMsg AckMessage
		err = json.Unmarshal(bufIn[:n], &ackMsg)
		checkError(err)
		logger.Println(fmt.Sprintf("makeRequest - Received heartbeat [%s]", string(bufIn[:n])))

		// TODO update RTT

		// Check ack is for the correct heartbeat
		if ackMsg.HBEatEpochNonce == monitor.epochNonce && ackMsg.HBEatSeqNum == msg.SeqNum {
			logger.Println("makeRequest - Success! Received matching ack")
			logger.Println("makeRequest - Resetting attempts and deadline")
			success <- struct{}{}
			return
		}
	}

	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		logger.Println("startMonitor - Failed to get Ack")
		return
	}
}