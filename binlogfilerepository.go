package main

import (
	"io/ioutil"
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

type BinLogFileRepository struct {
	sync.Mutex
	FileName string
}

func (b *BinLogFileRepository) SaveLastPosition(binLogPosition *BinLogPosition) error {
	b.Lock()
	defer b.Unlock()
	if binLogPosition.Position == 0 {
		return nil
	}

	data, err := yaml.Marshal(binLogPosition)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(b.FileName, data, 0666)
	if err != nil {
		return err
	}

	return nil
}

func (b *BinLogFileRepository) GetLastPosition() (*BinLogPosition, error) {
	b.Lock()
	defer b.Unlock()

	if _, err := os.Stat(b.FileName); err != nil {
		return nil, nil
	}

	yamlFile, err := ioutil.ReadFile(b.FileName)
	if err != nil {
		return nil, err
	}

	var binlogPosition BinLogPosition

	err = yaml.Unmarshal(yamlFile, &binlogPosition)
	if err != nil {
		return nil, err
	}

	return &binlogPosition, nil
}
func NewBinLogFileRepository(filename string) (*BinLogFileRepository, error) {
	repo := &BinLogFileRepository{
		Mutex:    sync.Mutex{},
		FileName: filename,
	}
	return repo, nil
}
