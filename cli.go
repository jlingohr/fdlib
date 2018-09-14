package main

import (
	"./fdlib"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var context *FDContext

type InvalidCommandError string

func (e InvalidCommandError) Error() string {
	return fmt.Sprintf("CLI: Invalid command [%s]", string(e))
}

type BadArgumentError string

func (e BadArgumentError) Error() string {
	return fmt.Sprintf("CLI: Invalid Argument for command [%s]", string(e))
}

type Cmd interface {
	Run(context *FDContext) error
}

type InitCmd struct {
	EpochNonce uint64
	ChCapacity uint8
}

type StartRespondingCmd struct {
	LocalIp string
}

type StopRespondingCmd struct{}

type AddMonitorCmd struct {
	LocalIP string
	RemoteIP string
	LostMsgThresh uint8
}


type RemoveMonitorCmd struct {
	RemoteIP string
}

type StopMonitoringCmd struct{}

type FDContext struct {
	fd fdlib.FD
	notifyChan <-chan fdlib.FailureDetected
}

var INITIALIZE = "Initialize"
var STARTRESPONDING = "StartResponding"
var STOPRESPONDING = "StopResponding"
var ADDMONITOR = "AddMonitor"
var REMOVEMONITOR = "RemoveMonitor"
var STOPMONITORING = "StopMonitoring"

func (cmd InitCmd) Run(context *FDContext) (err error) {
	fd, notifyChan, err := fdlib.Initialize(cmd.EpochNonce, cmd.ChCapacity)
	if checkErr(err) == nil {
		context.fd = fd
		context.notifyChan = notifyChan
	}
	return
	//runChan <- err
}
func (cmd StartRespondingCmd) Run(context *FDContext) (err error) {
	err = context.fd.StartResponding(cmd.LocalIp)
	return
	//runChan <- err
}

func (cmd StopRespondingCmd) Run(context *FDContext) (err error) {
	context.fd.StopResponding()
	return
	//runChan <- nil
}
func (cmd AddMonitorCmd) Run(context *FDContext) (err error) {
	err = context.fd.AddMonitor(cmd.LocalIP, cmd.RemoteIP, cmd.LostMsgThresh)
	return
	//runChan <- err
}
func (cmd RemoveMonitorCmd) Run(context *FDContext) (err error) {
	context.fd.RemoveMonitor(cmd.RemoteIP)
	return
	//runChan <- nil
}
func (cmd StopMonitoringCmd) Run(context *FDContext)  (err error) {
	context.fd.StopMonitoring()
	return
	//runChan <- nil
}


func main() {
	fmt.Println("Starting interactive shell for fdlib")

	context = newContext()

	cmdChan := make(chan Cmd)
	//runChan := make(chan error)

	go runContext(context, cmdChan)
	go readConsole(cmdChan)

	select {}

}

func parseInput(line string) (command Cmd, err error) {
	split := strings.Split(line, " ")
	cmdString := split[0]
	args := split[1:]

	switch {
	case cmdString == INITIALIZE:
		command, err = parseInit(args)
	case cmdString == STARTRESPONDING:
		command, err = parseStartResponding(args)
	case cmdString == STOPRESPONDING:
		command, err = parseStopResponding(args)
	case cmdString == ADDMONITOR:
		command, err = parseAddMonitor(args)
	case cmdString == REMOVEMONITOR:
		command, err = parseRemoveMonitor(args)
	case cmdString == STOPMONITORING:
		command, err = parseStopMonitoring(args)
	default:
		err = InvalidCommandError(cmdString)
	}
	return
}

func parseStopMonitoring(args []string) (command Cmd, err error) {
	if len(args) > 0 {
		err = BadArgumentError(STOPMONITORING)
	} else {
		command = StopMonitoringCmd{}
	}
	return
}

func parseRemoveMonitor(args []string) (command Cmd, err error) {
	if len(args) != 1 {
		err = BadArgumentError(REMOVEMONITOR)
	} else {
		command = RemoveMonitorCmd{args[0]}
	}
	return
}

func parseAddMonitor(args []string) (command Cmd, err error) {
	if len(args) != 3 {
		err = BadArgumentError(ADDMONITOR)
	} else {
		localIP, remoteIP := args[0], args[1]
		msgThresh, err := strconv.ParseUint(args[2], 10, 8)
		if err == nil {
			command = AddMonitorCmd{localIP, remoteIP, uint8(msgThresh)}
		}
	}
	return
}

func parseStopResponding(args []string) (command Cmd, err error) {
	if len(args) > 0 {
		err = BadArgumentError(STOPRESPONDING)
	} else {
		command = StopRespondingCmd{}
	}
	return
}

func parseStartResponding(args []string) (command Cmd, err error) {
	if len(args) != 1 {
		err = BadArgumentError(STOPRESPONDING)
	} else {
		command = StartRespondingCmd{args[0]}
	}
	return
}

func parseInit(args []string) (command Cmd, err error) {
	if len(args) != 2 {
		err = BadArgumentError(INITIALIZE)
	} else {
		nonce, _ := strconv.ParseUint(args[0], 10, 64)
		capacity, err := strconv.ParseUint(args[1], 10, 8)
		//if _, err := fmt.Scan(&nonce, &capacity); err != nil {
		//	command = InitCmd{nonce, uint8(capacity)}
		//}
		if err == nil {
			command = InitCmd{nonce, uint8(capacity)}
		}
	}
	return
}

func newContext() (ctxt *FDContext) {
	ctxt = &FDContext{}
	return
}

func checkErr(err error) error {
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}

func readConsole(ch chan Cmd) {
	//reader := bufio.NewReader(os.Stdin)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		line := scanner.Text()
		command, err := parseInput(line)
		checkErr(err)
		if err == nil {
			ch <- command
		}
	}
}

func runContext(context *FDContext, cmdChan chan Cmd) {
	for {
		select {
		case cmd := <- cmdChan:
			err := cmd.Run(context)
			checkErr(err)
		}
	}

}