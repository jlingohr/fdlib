/*

This package specifies the API to the failure detector library to be
used in assignment 1 of UBC CS 416 2018W1.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Initialize, but you
cannot change its API.

*/

package fdlib

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

//////////////////////////////////////////////////////
// Define the message types fdlib has to use to communicate to other
// fdlib instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fdlib instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
}

//////////////////////////////////////////////////////

// An FD interface represents an instance of the fd
// library. Interfaces are everywhere in Go:
// https://gobyexample.com/interfaces
type FD interface {
	// Tells the library to start responding to heartbeat messages on
	// a local UDP IP:port. Can return an error that is related to the
	// underlying UDP connection.
	StartResponding(LocalIpPort string) (err error)

	// Tells the library to stop responding to heartbeat
	// messages. Always succeeds.
	StopResponding()

	// Tells the library to start monitors a particular UDP IP:port
	// with a specific lost messages threshold. Can return an error
	// that is related to the underlying UDP connection.
	AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error)

	// Tells the library to stop monitors a particular remote UDP
	// IP:port. Always succeeds.
	RemoveMonitor(RemoteIpPort string)

	// Tells the library to stop monitors all nodes.
	StopMonitoring()
}

///////////////////////
// Errors

type SocketWriteError string

func (e SocketWriteError) Error() string {
	return fmt.Sprintf("Writing to connection - [%s]", string(e))
}

type SocketReadError string

func (e SocketReadError) Error() string {
	return fmt.Sprintf("Reading from connection - [%s]", string(e))
}

type InitializeError string

func (e InitializeError) Error() string {
	return fmt.Sprintf("%s", string(e))
}



type Detector struct {
	epochNonce     uint64
	notifyChan     chan FailureDetected
	monitors       map[string]*Monitor // Map from remote IP:PORT to Monitor
	respondingChan chan struct{}       // Channel the local listens to
	remoteToMonitorMap map[string][]*Monitor // Map from remote address to address of monitor
	startRespondingChan chan string
	errorChan chan error
	continueChan chan struct{}
}

// The constructor for a new FD object instance. Note that notifyCh
// can only be received on by the client that receives it from
// initialize:
// https://www.golang-book.com/books/intro/10
func Initialize(EpochNonce uint64, ChCapacity uint8) (fd FD, notifyCh <-chan FailureDetected, err error) {
	// TODO
	if FDLib == nil {
		failureChan := make(chan FailureDetected, ChCapacity)
		notifyCh = failureChan
		//failureDetectedChan := make(chan FailureDetected, ChCapacity)
		respondingChan := make(chan struct{})
		monitoring := make(map[string]*Monitor)
		remoteToMonitorMap := make(map[string][]*Monitor)
		startRespondingChan := make(chan string)
		errorChan := make(chan error)
		continueChan := make(chan struct{})

		FDLib = &Detector{EpochNonce, failureChan, monitoring, respondingChan, remoteToMonitorMap, startRespondingChan, errorChan, continueChan}
		go runFDLIb(FDLib)
		fd = FDLib
		return
	} else {
		fd = nil
		notifyCh = nil
		err = InitializeError("Multiple calls to Initialize not allowed.")
		return
	}

}

func runFDLIb(fd *Detector) {
	var conn *net.UDPConn
	//var err error
Outter:
	for {
		select {
		case ip := <- fd.startRespondingChan:
			//if conn != nil {
			//	err = errors.New("Cannot make consecutive calls to StartResponding")
			//	fd.errorChan <- err
			//	continue
			//}
			localIpPort := ip
			lAddr, err := net.ResolveUDPAddr("udp", localIpPort)
			if err != nil {
				fd.errorChan <- err
				continue Outter
			}
			conn, err = net.ListenUDP("udp", lAddr)
			if err != nil {
				fd.errorChan <- err
				continue Outter
			}
			fd.continueChan <- struct{}{}
		Inner:
			for {
				bufIn := make([]byte, 1024)
				//logger.Println(fmt.Sprintf("StartResponding - Waiting for heartbeats on [%s]", localIpPort))
				select {
				case <- fd.startRespondingChan:
					err = errors.New("Cannot make consecutive calls to StartResponding")
					fd.errorChan <- err
					continue Inner

				case <- fd.respondingChan:
					//logger.Println(fmt.Sprintf("StartResponding - No longer responding on [%s]", localIpPort))
					f, _ := conn.File()
					f.Close()
					conn.Close()
					conn = nil
					fd.continueChan <- struct{}{}
					break Inner

				default:
					n, rAddr, err := conn.ReadFromUDP(bufIn)
					checkError(err)

					// Handle heartbeat
					bufDecoder := bytes.NewBuffer(bufIn[:n])
					decoder := gob.NewDecoder(bufDecoder)
					hbeatMsg, err := decodeHBeat(decoder)
					if checkError(err) {
						continue Inner
					}

					//logger.Println(fmt.Sprintf("StartResponding - Received heartbeat on [%s] from [%s]", LocalIpPort, rAddr.String()))

					// Respond with Ack
					ackMsg := AckMessage{hbeatMsg.EpochNonce, hbeatMsg.SeqNum}
					bufOut, err := encodeAck(ackMsg)
					if checkError(err) {
						continue Inner
					}
					_, err = conn.WriteToUDP(bufOut, rAddr)
					checkError(err)
				}
			}
		case <- fd.respondingChan:
			logger.Println("Stopping")
			fd.continueChan <- struct{}{}
			continue Outter
		}
	}

}

/////////////
// fdlib implementation

// Tells the library to start responding to heartbeat messages on
// a local UDP IP:port. Can return an error that is related to the
// underlying UDP connection.
func (fd Detector) StartResponding(LocalIpPort string) (err error) {
	fd.startRespondingChan <- LocalIpPort
	select {
	case <- fd.continueChan:
		return
	case err = <- fd.errorChan:
		return
	}
	return
}

// Tells the library to stop responding to heartbeat
// messages. Always succeeds.
func (fd Detector) StopResponding() {
	//logger.Println("StopResponding - No longer responding to heartbeats")
	fd.respondingChan <- struct{}{}
	<- fd.continueChan
	return
}


// Tells the library to start monitors a particular UDP IP:port
// with a specific lost messages threshold. Can return an error
// that is related to the underlying UDP connection.
func (fd Detector) AddMonitor(LocalIpPort string, RemoteIpPort string, LostMsgThresh uint8) (err error) {
	peer, err := NewPeer(RemoteIpPort, LostMsgThresh)
	if checkError(err) {
		return
	}
	monitor, contains := fd.monitors[LocalIpPort]
	if !contains {
		// Create new Monitor
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
	//logger.Println("StopMonitoring - Stopping all heartbeats...")
	for remoteIP, _ := range(fd.remoteToMonitorMap) {
		//fd.RemoveMonitor(monitor.Conn.RemoteAddr().String())
		fd.RemoveMonitor(remoteIP)
	}
	return
}

/////////////
// Helper structs and functions

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
	Running bool
	isRunningMux sync.Mutex
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
	p.delayMux.Lock()
	defer p.delayMux.Unlock()
	requestTime := end.Sub(start)
	newDelay := (p.Delay + requestTime)/2
	p.Delay = newDelay
	//logger.Println(fmt.Sprintf("updateRTT - New delay is [%f]", p.Delay.Seconds()))
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

func (p *Peer) IsRunning() bool {
	p.isRunningMux.Lock()
	defer p.isRunningMux.Unlock()
	return p.Running
}

func (p *Peer) SetRunning(value bool) {
	p.isRunningMux.Lock()
	defer p.isRunningMux.Unlock()
	p.Running = value
	return
}

var (
	sequenceNumber *SequenceNumber = newSeqNumber()
)

type SequenceNumber struct {
	SeqNum uint64
	mux sync.Mutex
}

func newSeqNumber() *SequenceNumber {
	return &SequenceNumber{SeqNum: 0}
}

func (s *SequenceNumber) getSeqNum() uint64 {
	s.mux.Lock()
	defer s.mux.Unlock()
	num := s.SeqNum
	s.SeqNum++
	//logger.Println(fmt.Sprintf("getSeqNum - New sequence number is [%d]", s.SeqNum))
	return num
}

type Ack struct {
	Address string
	HBeatAck
}

type HBeatAck struct {
	Buffer []byte
}

type Request struct {
	buffer []byte
	Address *net.UDPAddr
}

type HBeatRequest struct {
	RequestStartTime time.Time
	HBeatMessage
}

type NewListener struct {
	Address string
	AckChan chan HBeatAck
}

// Monitors send HBeats from a local address to various remote addresses
type Monitor struct {
	EpochNonce   uint64
	Conn         *net.UDPConn
	Peers        map[string]*Peer // Thresholds for each peer
	NewPeersChan chan *Peer       // Channel to receive new nodes to monitor
	RemovePeersChan chan string
}

func NewMonitor(localAddress string, epochNonce uint64) (monitor *Monitor, err error) {
	lAddr, err := net.ResolveUDPAddr("udp", localAddress)
	if checkError(err) {
		return
	}

	conn, err := net.ListenUDP("udp", lAddr)
	if checkError(err) {
		return
	}

	peers := make(map[string]*Peer)
	peersChan := make(chan *Peer)
	removePeersChan := make(chan string)
	monitor = &Monitor{
		epochNonce,
		conn,
		peers,
		peersChan,
		removePeersChan,
	}
	return
}


func RunMonitor(monitor *Monitor, notifyChan chan<- FailureDetected) {

	stopListening := make(chan struct{})
	stopSending := make(chan struct{})
	stopHBeat := make(chan string)

	requestChan := make(chan Request) 
	newListenerChan := make(chan NewListener)
	removeListenerChan := make(chan string)
	go ListenHBeats(monitor, stopListening, newListenerChan, removeListenerChan) // All responses read through here first
	go SendHBeats(monitor, stopSending, requestChan) // All requests made through here

	for {
		select {
		case newPeer := <- monitor.NewPeersChan:
			// Add or update peer
			peerAddress := newPeer.Address.String()
			peer, contains := monitor.Peers[peerAddress]
			if !contains {
				monitor.Peers[peerAddress] = newPeer
				peer = newPeer
				newListener := NewListener{peerAddress, make(chan HBeatAck)}
				newListenerChan <- newListener // Register a new listener
				go StartHBeat(monitor.EpochNonce, peer, requestChan, newListener.AckChan, notifyChan, stopHBeat)
			} else {
				peer.UpdateThreshold(newPeer.Threshold)
				if !peer.IsRunning()  {
					newListener := NewListener{peerAddress, make(chan HBeatAck)}
					newListenerChan <- newListener // Register a new listener
					go StartHBeat(monitor.EpochNonce, peer, requestChan, newListener.AckChan, notifyChan, stopHBeat)
				}
			}
		case removeIP := <- monitor.RemovePeersChan:
			logger.Println(fmt.Sprintf("Remove [%s]", removeIP))
			removeListenerChan <- removeIP //Stop listening for acks
			stopHBeat <- removeIP
		}
	}
}


// Responsible for sending all heartbeats for a monitors local IP:Port
func SendHBeats(monitor *Monitor, stopSending chan struct{}, requestChan chan Request) {
	for {
		select {
		case <- stopSending:
			//logger.Println(fmt.Sprintf("SendHBeats - Stopping requests on [%s]", monitor.Conn.LocalAddr().String()))
			return
		case request := <-requestChan:
			logger.Println(fmt.Sprintf("SendHBeats - Sending request on [%s] to [%s]", monitor.Conn.LocalAddr().String(), request.Address))
			_, err := monitor.Conn.WriteToUDP(request.buffer, request.Address)
			if err != nil {
				checkError(SocketWriteError(err.Error()))
			}

		}
	}
}



// Responsible for listening for all heartbeat acks for a monitors local IP:Port and forwards to channel
// corresponding to a remote IP:Port
func ListenHBeats(monitor *Monitor, stopListening chan struct{}, newListenerChan chan NewListener, removeListenerChan chan string) {

	monitors := make(map[string]chan HBeatAck) // Map of localIps to monitor channel
	ackChan := listenHBeats(monitor.Conn, stopListening)
	for {
		select {
		case <- stopListening:
			return
		case newListener := <- newListenerChan:
			monitors[newListener.Address] = newListener.AckChan
		case id := <- removeListenerChan:
			delete(monitors, id)
		case ack := <- ackChan:
			monitors[ack.Address] <- ack.HBeatAck
		}
	}
}

func listenHBeats(conn *net.UDPConn, stopListening chan struct{}) chan Ack {
	ackChan := make(chan Ack)
	go func() {
		for {
			buffer := make([]byte, 1024)
			select {
			case <- stopListening:
				return //TOOD not ever used
			default:
				n, rAddr, err := conn.ReadFromUDP(buffer)
				if err != nil {
					checkError(SocketReadError(err.Error()))
					continue
				}
				if n > 0 {
					bufOut := buffer[:n]
					ackChan <- Ack{rAddr.String(), HBeatAck{bufOut}}
				}
			}
		}
	}()
	return ackChan
}

// Handles HBeats by sending new messages over the channel and receiving any acks back
func StartHBeat(epochNonce uint64, peer *Peer, requestChan chan Request, ackChan chan HBeatAck, notifyChan chan<- FailureDetected, stopHBeat chan string) {
	outstandingMsgs := uint8(0)
	outstandingRequests := make(map[uint64]HBeatRequest)
	done := make(chan struct{})
	for outstandingMsgs < peer.GetThreshold() {
		//logger.Println(fmt.Sprintf("StartHBeat - outstanding messages: [%d]", outstandingMsgs))
		select {
		case id := <- stopHBeat:
			if id == peer.Address.String() {
				done <- struct{}{}
			}
		default:
			delay := peer.getDelay()
			seqNum := sequenceNumber.getSeqNum()
			hbeatMsg := HBeatMessage{epochNonce, seqNum}
			bufOut, err := encodeHBeat(hbeatMsg)
			if checkError(err) {
				continue
			}

			request := Request{bufOut, peer.Address}
			hbeatRequest := HBeatRequest{time.Now(), hbeatMsg}
			outstandingRequests[seqNum] = hbeatRequest
			requestChan <- request // Send the request
			ticker := time.NewTicker(delay)
			defer ticker.Stop()

		Inner:
			for {
				select {
				case <- done:
					//logger.Println(fmt.Sprintf("StartHBeat - Stopping heartbeat for [%s]", peer.Address.String()))
					return
				case ack :=<- ackChan:
					// Respond to ack
					bufDecoder := bytes.NewBuffer(ack.Buffer)
					decoder := gob.NewDecoder(bufDecoder)
					ackMsg, err := decodeAck(decoder)
					//err := json.Unmarshal(ack.Buffer[:ack.N], &ackMsg)
					if checkError(err) {
						continue Inner
					}
					outstandingRequest, ok := outstandingRequests[ackMsg.HBEatSeqNum]
					if !ok {
						continue Inner
					}
					if outstandingRequest.EpochNonce == ackMsg.HBEatEpochNonce && outstandingRequest.SeqNum == ackMsg.HBEatSeqNum {
						//logger.Println(fmt.Sprintf("StartHBeat - Successfully received ack [%d] from [%s]", outstandingRequest.SeqNum, peer.Address))
						peer.UpdateDelay(outstandingRequest.RequestStartTime, time.Now())
						delete(outstandingRequests, ackMsg.HBEatSeqNum) // Remove from outstanding requests
						outstandingMsgs = 0
						break Inner
					}
				case <- ticker.C:
					//logger.Println(fmt.Sprintf("StartHbeat - Timeout monitoring [%s]", peer.Address.String()))
					outstandingMsgs += 1
					break Inner
				}
			}
		}
	}

	//TODO notify failure and shutdown
	currentTime := time.Now()
	failureDetectedMsg := FailureDetected{peer.Address.String(), currentTime}
	notifyChan <- failureDetectedMsg
	return

}

type EncodeHBeatError string
func (e EncodeHBeatError) Error() string {
	return fmt.Sprintf("Encoding HBeatMessage - [%s]", string(e))
}

type DecodeHBeatError string
func (e DecodeHBeatError) Error() string {
	return fmt.Sprintf("Decoding HBeatMessage - [%s]", string(e))
}

func encodeHBeat(hbeat HBeatMessage) ([]byte, error) {
	var network bytes.Buffer
	gob.Register(HBeatMessage{})
	enc := gob.NewEncoder(&network)
	err := enc.Encode(&hbeat)
	if err != nil {
		err = EncodeHBeatError(err.Error())
	}
	return network.Bytes(), err
}

func decodeHBeat(dec *gob.Decoder) (HBeatMessage, error) {
	var msg HBeatMessage
	err := dec.Decode(&msg)
	if err != nil {
		err = DecodeHBeatError(err.Error())
	}
	return msg, err
}


type EncodeAckError string
func (e EncodeAckError) Error() string {
	return fmt.Sprintf("Encoding AckMessage - [%s]", string(e))
}

type DecodeAckError string
func (e DecodeAckError) Error() string {
	return fmt.Sprintf("Decoding AckMessage - [%s]", string(e))
}

func encodeAck(ack AckMessage) ([]byte, error) {
	var network bytes.Buffer
	gob.Register(HBeatMessage{})
	enc := gob.NewEncoder(&network)
	err := enc.Encode(&ack)
	if err != nil {
		err = EncodeAckError(err.Error())
	}
	return network.Bytes(), err
}

func decodeAck(dec *gob.Decoder) (AckMessage, error) {
	var msg AckMessage
	err := dec.Decode(&msg)
	if err != nil {
		err = DecodeAckError(err.Error())
	}
	return msg, err
}
