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
	SeqNum uint64
	mux sync.Mutex
}

func newSeqNumber() *seqNumber {
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
	delayMux sync.Mutex
}


func NewMonitor(localAddress string, remoteAddress string, threshold uint8, epochNonce uint64) (monitor *Monitor, err error) {
	lAddr, err := net.ResolveUDPAddr("udp", localAddress)
	checkError(err) //TODO probably should just exit here
	rAddr, err := net.ResolveUDPAddr("udp", remoteAddress)
	killSwitch := make(chan struct{})
	initialDelay := 3 * time.Second
	delayMux := sync.Mutex{}

	monitor = &Monitor{lAddr, rAddr, threshold, epochNonce ,killSwitch, initialDelay, delayMux}

	logger.Println(fmt.Sprintf("NewMonitor - remote [%s], threshold [%d], nonce [%d]",
		monitor.RemoteAddress, monitor.Threshold, monitor.epochNonce))

	return
}

func StartMonitor(monitor *Monitor, notifyChan chan FailureDetected, failureChan chan FailureDetected) {

	detected := make(chan struct{})
	defer close(detected)

	go heartbeat(monitor, detected)

	select {
	case <- monitor.killSwitch:
		//TODO stop monitor
		logger.Println(fmt.Sprintf("StartMonitor - Killswitch hit. Shutting down monitor for remote [%s]", monitor.RemoteAddress))
		//monitor.Conn.Close()
		//monitor.killSwitch
		return
	case <- detected:
		logger.Println(fmt.Sprintf("StartMonitor - Failed to get any acks. Notifying failure for [%s]", monitor.RemoteAddress.String()))
		notifyChan <- notifyFailureDetected(monitor.RemoteAddress.String())
		close(failureChan)
		return
	}
}

func heartbeat(monitor *Monitor, detected chan struct{}) {
	logger.Println(fmt.Sprintf("heartbeat - Starting to monitor remote [%s]", monitor.RemoteAddress))
	conn, err := net.DialUDP("udp", monitor.LocalAddress, monitor.RemoteAddress)
	checkError(err)
	//defer conn.Close() //TODO
	lostMessages := uint8(0)
	success := make(chan struct{})
	seq := seqNum.getSeqNum()
	for lostMessages < monitor.Threshold {
		// Make request
		delay := monitor.getDelay()

		go makeRequest(conn, monitor, seq, delay, success)

		select {
		case <- time.After(delay): 
			lostMessages += 1
		case <- success:
			logger.Println("heartbeat - Resetting attempts and deadline")
			lostMessages = 0
			seq = seqNum.getSeqNum()
		}

	}

	detected <- struct{}{}
	return
}

func makeRequest(conn *net.UDPConn, monitor *Monitor, seqNum uint64, delay time.Duration, success chan struct{}) {

	//seqNum := seqNum.getSeqNum()
	hbeatMsg := HBeatMessage{monitor.epochNonce, seqNum}

	bufOut, err := json.Marshal(hbeatMsg)
	checkError(err)
	logger.Println(fmt.Sprintf(
		"makeRequest - Sending new heartbeat request [%s] to remote [%s] from [%s]",
		string(bufOut), monitor.RemoteAddress, monitor.LocalAddress))

	// Send request
	reqStartTime := time.Now() //TODO
	_, err = conn.Write(bufOut)

	// Read ack
	bufIn := make([]byte, 1024)
	n, err := conn.Read(bufIn)
	checkError(err)
	reqEndTime := time.Now()

	monitor.updateRTT(reqStartTime, reqEndTime)

	if n > 0 {
		var ackMsg AckMessage
		err = json.Unmarshal(bufIn[:n], &ackMsg)
		checkError(err)

		// Check ack is for the correct heartbeat
		if ackMsg.HBEatEpochNonce == hbeatMsg.EpochNonce && ackMsg.HBEatSeqNum == hbeatMsg.SeqNum {
			logger.Println("makeRequest - Success! Received matching ack")
			sleepTime := reqStartTime.Add(delay).Sub(reqEndTime)
			<- time.After(sleepTime)//TODO is this correct
			success <- struct{}{}
		} else {
			logger.Println(fmt.Sprintf("Expected nonce [%d] and seq [%d] but got nonce [%d] and seq [%d]",
				hbeatMsg.EpochNonce, hbeatMsg.SeqNum, ackMsg.HBEatEpochNonce, ackMsg.HBEatSeqNum))
		}
	}
	return
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