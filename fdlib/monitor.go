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

type Request struct {
	SeqNum uint64
	StartTime time.Time
	EndTime time.Time
}

type Monitor struct {
	Conn *net.UDPConn
	Threshold     uint8
	epochNonce    uint64
	killSwitch    chan struct{}
	delay         time.Duration
	delayMux sync.Mutex
}


func NewMonitor(localAddress string, remoteAddress string, threshold uint8, epochNonce uint64) (monitor *Monitor, err error) {
	lAddr, err := net.ResolveUDPAddr("udp", localAddress)
	if checkError(err) {
		return
	}
	rAddr, err := net.ResolveUDPAddr("udp", remoteAddress)
	if checkError(err) {
		return
	}
	conn, err := net.DialUDP("udp", lAddr, rAddr)
	if checkError(err) {
		return
	}
	killSwitch := make(chan struct{})
	initialDelay := 3 * time.Second
	delayMux := sync.Mutex{}

	monitor = &Monitor{conn,
	threshold,
	epochNonce ,
	killSwitch,
	initialDelay,
	delayMux}

	return
}

func StartMonitor(monitor *Monitor, notifyChan chan FailureDetected, failureChan chan FailureDetected) {

	detected := make(chan struct{})
	defer close(detected)

	go heartbeat(monitor, detected)

	select {
	case <- monitor.killSwitch:
		//TODO stop monitor
		logger.Println(fmt.Sprintf("StartMonitor - Killswitch hit. Shutting down monitor for remote [%s]", monitor.Conn.RemoteAddr().String()))
		monitor.Conn.Close()
		//monitor.killSwitch
		return
	case <- detected:
		logger.Println(fmt.Sprintf("StartMonitor - Failed to get any acks. Notifying failure for [%s]", monitor.Conn.RemoteAddr().String()))
		notifyChan <- notifyFailureDetected(monitor.Conn.RemoteAddr().String())
		close(failureChan)
		return
	}
}

func heartbeat(monitor *Monitor, failureDetected chan struct{}) {
	lostMessages := uint8(0)
	ackChannel := make(chan struct{})
	timeoutChan := make(chan struct{})
	seq := seqNum.getSeqNum()
	delay := monitor.getDelay()
	for lostMessages < monitor.Threshold {
		// Make request
		go heartbeatRequest(monitor, seq)
		monitor.Conn.SetReadDeadline(time.Now().Add(delay))
		go heartbeatResponse(monitor, seq, delay, ackChannel, timeoutChan) //TODO keep request and response seperte

		select {
		case <- timeoutChan:
			lostMessages += 1
		case <-ackChannel:
			logger.Println("heartbeat - Resetting attempts and deadline")
			lostMessages = 0
			seq = seqNum.getSeqNum()
		}
		delay = monitor.getDelay()

	}

	failureDetected <- struct{}{}
	return
}

func heartbeatRequest(monitor *Monitor, seqNum uint64) {
	//TODO need to change so can have at most n outstanding requests but when receives late can still update
	// i.e. make more like tcp
	// This means making the response look for specific seq nums
	// (recall seq nums in 317)
	hbeatMsg := HBeatMessage{monitor.epochNonce, seqNum}
	bufOut, err := json.Marshal(hbeatMsg)
	checkError(err)

	// Send request
	_, err = monitor.Conn.Write(bufOut)
	return
}

func heartbeatResponse(monitor *Monitor, seq uint64, delay time.Duration, ackChannel chan struct{}, timeoutChan chan struct{}) {
	//TODO should probably loop so if receive wrong seq num can continue reading
	reqStartTime := time.Now() //TODO
	// Read ack
	bufIn := make([]byte, 1024)

	for {
		n, err := monitor.Conn.Read(bufIn)
		checkError(err)

		if n > 0 {
			var ackMsg AckMessage
			err = json.Unmarshal(bufIn[:n], &ackMsg)
			checkError(err)

			// Check ack is for the correct heartbeat
			if ackMsg.HBEatEpochNonce == monitor.epochNonce && ackMsg.HBEatSeqNum == seq {
				reqEndTime := time.Now()
				monitor.updateRTT(reqStartTime, reqEndTime)
				logger.Println("heartbeatRequest - Success! Received matching ack")
				sleepTime := reqStartTime.Add(delay).Sub(reqEndTime)
				<- time.After(sleepTime)//TODO is this correct
				ackChannel <- struct{}{}
				return //TODO maybe should not return and just always be listening (use channels to update)
			} else {
				logger.Println(fmt.Sprintf("Expected nonce [%d] and seq [%d] but got nonce [%d] and seq [%d]",
					monitor.epochNonce, seq, ackMsg.HBEatEpochNonce, ackMsg.HBEatSeqNum))
			}
		}

		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			// Connection timeout
			timeoutChan <- struct{}{}
			return
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