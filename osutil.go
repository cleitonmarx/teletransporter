package main

import (
	"os"
	"os/signal"
	"syscall"
)

type OsUtil struct {
	signalChannel chan os.Signal
}

func (osu *OsUtil) WaitExitSignal() os.Signal {
	select {
	case receivedSignal := <-osu.signalChannel:
		return receivedSignal
	}
}

func NewOsUtil() *OsUtil {
	osu := &OsUtil{}
	osu.signalChannel = make(chan os.Signal, 1)
	signal.Notify(osu.signalChannel,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	return osu
}
