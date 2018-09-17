package fdlib

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

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
	logger.Println(fmt.Sprintf("getSeqNum - New sequence number is [%d]", s.SeqNum))
	return num
}

type Ack struct {
	Address string
	HBeatAck
}

type HBeatAck struct {
	Buffer []byte
	N int
	TimeReceived time.Time
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


func RunMonitor(monitor *Monitor, notifyChan chan FailureDetected) {

	stopListening := make(chan struct{})
	stopSending := make(chan struct{}) //TODO probably same chan as StopMonitoring
	stopHBeat := make(chan string)

	requestChan := make(chan Request) //TODO maybe buffer
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
			} else {
				peer.UpdateThreshold(newPeer.Threshold)
			}
			newListener := NewListener{peerAddress, make(chan HBeatAck)}
			newListenerChan <- newListener // Register a new listener
			go StartHBeat(monitor.EpochNonce, peer, requestChan, newListener.AckChan, notifyChan, stopHBeat)

		case removeIP := <- monitor.RemovePeersChan:
			removeListenerChan <- removeIP
		}
	}
}


// Responsible for sending all heartbeats for a monitors local IP:Port
func SendHBeats(monitor *Monitor, stopSending chan struct{}, requestChan chan Request) {
	for {
		select {
		case <- stopSending:
			return //TODO not ever used
		case request := <-requestChan:
			logger.Println(fmt.Sprintf("SendHBeats - Sending request on [%s] to [%s]", monitor.Conn.LocalAddr().String(), request.Address))
			_, err := monitor.Conn.WriteToUDP(request.buffer, request.Address)
			checkError(err)
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
			return //TODO not ever used because not shutdown
		case newListener := <- newListenerChan:
			monitors[newListener.Address] = newListener.AckChan //TODO might need sunchronization
		case id := <- removeListenerChan:
			//close(monitors[id]) //TODO this is wrong, since its used to receive acks
			delete(monitors, id)
			//TODO also call close on the channel?
		case ack := <- ackChan:
			monitors[ack.Address] <- ack.HBeatAck //TODO might be circular
		}
	}
}

func listenHBeats(conn *net.UDPConn, stopListening chan struct{}) chan Ack {
	ackChan := make(chan Ack)
	go func() {
		buffer := make([]byte, 1024)
		for {
			select {
			case <- stopListening:
				return //TOOD not ever used
			default:
				n, rAddr, err := conn.ReadFromUDP(buffer)
				if checkError(err) {
					continue
				}
				if n > 0 {
					ackChan <- Ack{rAddr.String(), HBeatAck{buffer, n, time.Now()}}
				}
			}
		}
	}()
	return ackChan
}

func StartHBeat(epochNonce uint64, peer *Peer, requestChan chan Request, ackChan chan HBeatAck, notifyChan chan FailureDetected, stopHBeat chan string) {
	outstandingMsgs := uint8(0)
	outstandingRequests := make(map[uint64]HBeatRequest)
	done := make(chan struct{})
	for outstandingMsgs < peer.GetThreshold() {
		logger.Println(fmt.Sprintf("StartHBeat - outstanding messages: [%d]", outstandingMsgs))
		select {
		case id := <- stopHBeat:
			if id == peer.Address.String() {
				done <- struct{}{}
			}
		default:
			delay := peer.getDelay()
			seqNum := sequenceNumber.getSeqNum()
			hbeatMsg := HBeatMessage{epochNonce, seqNum}
			bufOut, err := json.Marshal(hbeatMsg)
			if checkError(err) {
				continue //todo
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
					logger.Println(fmt.Sprintf("StartHBeat - Stopping heartbeat for [%s]", peer.Address.String()))
					return
				case ack :=<- ackChan:
					// Respond to ack
					var ackMsg AckMessage
					err := json.Unmarshal(ack.Buffer[:ack.N], &ackMsg)
					if checkError(err) {
						continue
					}
					outstandingRequest, ok := outstandingRequests[ackMsg.HBEatSeqNum]
					if !ok {
						continue
					}
					if outstandingRequest.EpochNonce == ackMsg.HBEatEpochNonce && outstandingRequest.SeqNum == ackMsg.HBEatSeqNum {
						logger.Println(fmt.Sprintf("StartHBeat - Successfully received ack [%d] from [%s]]", outstandingRequest.SeqNum, peer.Address))
						//ticker.Stop() //TODO do you need this
						peer.UpdateDelay(outstandingRequest.RequestStartTime, ack.TimeReceived)
						delete(outstandingRequests, ackMsg.HBEatSeqNum) // Remove from outstanding requests
						outstandingMsgs = 0
						<- ticker.C
						break Inner //TODO this might cause listener to block too much - maybe anouther routine so whenever break any outstanding still dealth with
					}
				case <- ticker.C:
					logger.Println(fmt.Sprintf("StartHbeat - Timeout monitoring [%s]", peer.Address.String()))
					outstandingMsgs += 1
					break Inner //TODO maybe send request here
				}
			}
		}
	}
	//TODO notify failure and shutdown
	notifyChan <- notifyFailureDetected(peer.Address.String())

}

func notifyFailureDetected(remoteAddr string) (failureDetectedMsg FailureDetected) {
	currentTime := time.Now()
	failureDetectedMsg = FailureDetected{remoteAddr, currentTime}
	return
}
