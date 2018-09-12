package fdlib

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	seqNum *seqNumber = newSeqNumber()
)

type seqNumber struct {
	SeqNum uint64 // TODO should be initialized randomly
	mux sync.Mutex
}

func newSeqNumber() *seqNumber {
	//return seqNumber{SeqNum: rand.Uint64()}
	return &seqNumber{SeqNum: 0}
}

func (s *seqNumber) getSeqNum() uint64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	num := s.SeqNum
	s.SeqNum++
	logger.Println(fmt.Sprintf("getSeqNum - New sequence number is [%d]", s.SeqNum))
	return num
}

type Monitor struct {
	LocalAddress *net.UDPAddr
	RemoteAddress *net.UDPAddr
	Threshold     uint8
	epochNonce    uint64
	killSwitch    chan struct{}
	delay         time.Duration
	//Conn          *net.UDPConn
	delayMux sync.Mutex
}


func NewMonitor(localAddress string, remoteAddress string, threshold uint8, epochNonce uint64) (monitor *Monitor, err error) {
	lAddr, err := net.ResolveUDPAddr("udp", localAddress)
	checkError(err) //TODO probably should just exit here
	rAddr, err := net.ResolveUDPAddr("udp", remoteAddress)
	// Connect to the remote node
	//conn, err := net.DialUDP("udp", lAddr, rAddr)
	//checkError(err)

	killSwitch := make(chan struct{})
	initialDelay := 3 * time.Second
	delayMux := sync.Mutex{}
	monitor = &Monitor{lAddr, rAddr, threshold, epochNonce ,killSwitch, initialDelay, delayMux}
	logger.Println(fmt.Sprintf("NewMonitor - remote [%s], threshold [%d], nonce [%d]",
		monitor.RemoteAddress, monitor.Threshold, monitor.epochNonce))
	return
}

func StartMonitor(monitor *Monitor, notifyChan chan FailureDetected) {

	detected := make(chan struct{})
	defer close(detected)

	go heartbeat(monitor, detected)

	select {
	case <- monitor.killSwitch:
		//TODO stop monitor
		logger.Println(fmt.Sprintf("StartMonitor - Killswitch hit. Shutting down monitor for remote [%s]", monitor.RemoteAddress))
		//monitor.Conn.Close()
		close(monitor.killSwitch)
		return
	case <- detected:
		logger.Println(fmt.Sprintf("StartMonitor - Failed to get any acks. Notifying failure for [%s]", monitor.RemoteAddress.String()))
		notifyChan <- notifyFailureDetected(monitor.RemoteAddress.String())
	}
}

func heartbeat(monitor *Monitor, detected chan struct{}) {
	logger.Println(fmt.Sprintf("heartbeat - Starting to monitor remote [%s]", monitor.RemoteAddress))
	lostMessages := uint8(0)
	success := make(chan struct{})
	for lostMessages < monitor.Threshold {
		// Make request
		delay := monitor.getDelay()
		seqNum := seqNum.getSeqNum()

		go makeRequest(monitor, seqNum, success)

		select {
		case <- time.After(delay): //TODO
			lostMessages += 1
		case <- success:
			logger.Println("heartbeat - Resetting attempts and deadline")
			lostMessages = 0
		}
	}

	detected <- struct{}{}
	return
}

func makeRequest(monitor *Monitor, seqNum uint64, success chan struct{}) {
	conn, err := net.DialUDP("udp", monitor.LocalAddress, monitor.RemoteAddress)
	checkError(err)
	defer conn.Close()
	//seqNum := seqNum.getSeqNum()
	hbeatMsg := HBeatMessage{monitor.epochNonce, seqNum}

	bufOut, err := json.Marshal(hbeatMsg)
	checkError(err)
	logger.Println(fmt.Sprintf(
		"makeRequest - Sending new heartbeat request [%s] to remote [%s]",
		string(bufOut), monitor.RemoteAddress))

	// Send request
	reqStartTime := time.Now() //TODO
	_, err = conn.Write(bufOut)
	logger.Println(fmt.Sprintf("makeRequest - Successfully send heartbeat to [%s]", monitor.RemoteAddress.String()))

	// Read ack
	bufIn := make([]byte, 1024)
	logger.Println(fmt.Sprintf("makeRequest - Waiting for ack from [%s]", monitor.RemoteAddress.String()))
	n, err := conn.Read(bufIn)
	checkError(err)
	reqEndTime := time.Now() //TODO
	logger.Println(fmt.Sprintf("makeRequest - Received message [%s] from [%s]", string(bufIn[:n]), monitor.RemoteAddress))

	monitor.updateRTT(reqStartTime, reqEndTime)

	if n > 0 {
		var ackMsg AckMessage
		err = json.Unmarshal(bufIn[:n], &ackMsg)
		checkError(err)

		// TODO update RTT

		// Check ack is for the correct heartbeat
		if ackMsg.HBEatEpochNonce == hbeatMsg.EpochNonce && ackMsg.HBEatSeqNum == hbeatMsg.SeqNum {
			logger.Println("makeRequest - Success! Received matching ack")
			success <- struct{}{}
			return
		} else {
			logger.Println(fmt.Sprintf("Expected nonce [%d] and seq [%d] but got nonce [%d] and seq [%d]",
				hbeatMsg.EpochNonce, hbeatMsg.SeqNum, ackMsg.HBEatEpochNonce, ackMsg.HBEatSeqNum))
		}
	}
}

func (monitor *Monitor) updateRTT(reqStartTime time.Time, reqEndTime time.Time) {
	requestTime := reqEndTime.Sub(reqStartTime)
	newDelay := (monitor.getDelay() + requestTime)/2
	monitor.delay = newDelay
	logger.Println(fmt.Sprintf("updateRTT - New delay is [%f]", monitor.delay.Seconds()))
}

func notifyFailureDetected(remoteAddr string) (failureDetectedMsg FailureDetected) {
	currentTime := time.Now()
	failureDetectedMsg = FailureDetected{remoteAddr, currentTime}
	return
}

func (monitor *Monitor) getDelay() time.Duration {
	monitor.delayMux.Lock()
	defer monitor.delayMux.Unlock()
	return monitor.delay
}