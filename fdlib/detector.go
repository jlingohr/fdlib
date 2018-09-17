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

type Peer struct {
	Address *net.UDPAddr
	Threshold uint8
	Delay time.Duration
	delayMux sync.Mutex
	thresholdMux sync.Mutex
}

func NewPeer(remoteAddress string, lostMsgThreshold uint8) (peer *Peer, err error) {
	rAddr, err := net.ResolveUDPAddr("udp", remoteAddress)
	if checkError(err) {
		return
	}

	peer = &Peer{Address: rAddr, Threshold: lostMsgThreshold, Delay: 3*time.Second}
	return
}

func (p *Peer) getDelay() time.Duration {
	p.delayMux.Lock()
	defer p.delayMux.Unlock()
	return p.Delay
}

func (p *Peer) UpdateDelay(start time.Time, end time.Time) {
	requestTime := end.Sub(start)
	newDelay := (p.getDelay() + requestTime)/2
	p.delayMux.Lock()
	defer p.delayMux.Unlock()
	p.Delay = newDelay
	logger.Println(fmt.Sprintf("updateRTT - New delay is [%f]", p.Delay.Seconds()))
	return
}

func (p *Peer) GetThreshold() uint8 {
	p.thresholdMux.Lock()
	defer p.thresholdMux.Unlock()
	return p.Threshold
}

func (p *Peer) UpdateThreshold(newThreshold uint8) {
	p.thresholdMux.Lock()
	defer p.thresholdMux.Unlock()
	if newThreshold != p.Threshold {
		p.Threshold = newThreshold
	}
	return
}

type Detector struct {
	epochNonce     uint64
	notifyChan     chan FailureDetected
	monitors       map[string]*Monitor // Map from remote IP:PORT to Monitor
	respondingChan chan struct{}       // Channel the local listens to
	remoteToMonitorMap map[string][]*Monitor // Map from remote address to address of monitor
}

/////////////
// fdlib instance constructor

func CreateDetector(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	notifyChan := make(chan FailureDetected, ChCapacity)
	respondingChan := make(chan struct{})
	monitoring := make(map[string]*Monitor)
	remoteToMonitorMap := make(map[string][]*Monitor)
	//sequenceNumber := newSeqNum()

	fd = Detector{EpochNonce, notifyChan, monitoring, respondingChan, remoteToMonitorMap}
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


// Tells the library to start monitors a particular UDP IP:port
// with a specific lost messages threshold. Can return an error
// that is related to the underlying UDP connection.
func (fd Detector) AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error) {
	logger.Println(fmt.Sprintf(
		"AddMonitor - LocalIpPort [%s], RemoteIpPort [%s], LostMsgThres [%d]",
		LocalIpPort,
		RemoteIpPort,
		LostMsgThresh))

	//id := LocalIpPort + "::" + RemoteIpPort
	peer, err := NewPeer(RemoteIpPort, LostMsgThresh)
	if checkError(err) {
		return
	}
	monitor, contains := fd.monitors[LocalIpPort]
	if !contains {
		// Create new Monitor

		logger.Println("AddMonitor - Adding supervisee to startMonitor")
		monitor, err = NewMonitor(LocalIpPort, fd.epochNonce)

		if checkError(err) {
			return
		}
		fd.monitors[LocalIpPort] = monitor
		fd.remoteToMonitorMap[RemoteIpPort] = append(fd.remoteToMonitorMap[RemoteIpPort], monitor)
		go RunMonitor(monitor, fd.notifyChan)
	}
	monitor.NewPeersChan <- peer
	return
}

// Tells the library to stop monitors a particular remote UDP
// IP:port. Always succeeds.
func (fd Detector) RemoveMonitor(RemoteIpPort string) {
	monitors, contains := fd.remoteToMonitorMap[RemoteIpPort]
	if !contains {
		return
	}
	for _, monitor := range monitors {
		monitor.RemovePeersChan <- RemoteIpPort
	}
	return
}

// Tells the library to stop monitors all nodes.
func (fd Detector) StopMonitoring() {
	logger.Println("StopMonitoring - Stopping all heartbeats...")
	for remoteIP, _ := range(fd.remoteToMonitorMap) {
		//fd.RemoveMonitor(monitor.Conn.RemoteAddr().String())
		fd.RemoveMonitor(remoteIP)
	}
	return
}

func (fd Detector) Stop() {
	return
}
//////////////////
// Private methods



//func  startMonitor(monitor *Monitor, notifyChan chan FailureDetected) {
//
//	go sendHeartbeats(monitor.Conn)
//	go listenAcks(monitor)
//
//
//}




