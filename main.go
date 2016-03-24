package main

import "github.com/davecgh/go-spew/spew"

func main() {

	binLogFileRepo, _ := NewBinLogFileRepository("binlogfilerepo.yaml")
	config := NewRowEventConsumerConfig("192.168.99.100", 3306, "root", "picatic", nil)
	//config := NewRowEventConsumerConfig("192.168.99.100", 3306, "root", "picatic", &BinLogPosition{LogFile: "binlog.000001", Position: 4})

	rowEventConsumer, err := NewRowEventConsumer(config, binLogFileRepo)
	if err != nil {
		panic(err)
	}

	rowEventConsumer.OnChangedRowEvent = func(event *ChangedRowEvent) {
		spew.Dump(event)
	}

	if err := rowEventConsumer.Start(); err != nil {
		panic(err)
	}

	defer rowEventConsumer.Close()

	NewOsUtil().WaitExitSignal()
}
