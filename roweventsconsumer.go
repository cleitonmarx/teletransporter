package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

type BinLogPosition struct {
	LogFile  string
	Position int
}

type BinLogStateRepository interface {
	SaveLastPosition(binLogPosition *BinLogPosition) error
	GetLastPosition() (*BinLogPosition, error)
}

type RowEventConsumerConfig struct {
	Host        string
	Port        int
	User        string
	Password    string
	LogPosition *BinLogPosition
}

func NewRowEventConsumerConfig(host string, port int, user string, password string, logPosition *BinLogPosition) RowEventConsumerConfig {
	return RowEventConsumerConfig{
		Host:        host,
		Port:        port,
		User:        user,
		Password:    password,
		LogPosition: logPosition,
	}
}

//RowEventConsumer is this
type RowEventConsumer struct {
	connection        *client.Conn
	syncer            *replication.BinlogSyncer
	streamer          *replication.BinlogStreamer
	stopped           bool
	schemaCache       map[string]*schema.Table
	stateRepository   BinLogStateRepository
	Config            RowEventConsumerConfig
	CurrentPosition   *BinLogPosition
	OnChangedRowEvent func(event *ChangedRowEvent)
}

//Init is this
func (m *RowEventConsumer) init() error {
	var (
		err error
	)

	m.stopped = true
	if m.connection, err = client.Connect(
		fmt.Sprintf("%s:%d", m.Config.Host, m.Config.Port),
		m.Config.User,
		m.Config.Password,
		"",
	); err != nil {
		return err
	}

	m.syncer = replication.NewBinlogSyncer(uint32(rand.Intn(1000))+1001, "mysql")
	if err = m.syncer.RegisterSlave(
		m.Config.Host, uint16(m.Config.Port), m.Config.User, m.Config.Password,
	); err != nil {
		return err
	}

	if err = m.verifyBinLogFormat(); err != nil {
		return err
	}

	position, err := m.GetPosition()
	if err != nil {
		return err
	}
	m.CurrentPosition = &BinLogPosition{LogFile: position.Name, Position: int(position.Pos)}

	// Start sync with sepcified binlog file and position
	log.Printf("Start Sync in Binlog File:%s - Position: %d", position.Name, position.Pos)
	if m.streamer, err = m.syncer.StartSync(position); err != nil {
		return err
	}

	m.schemaCache = make(map[string]*schema.Table)

	return nil
}

func (m *RowEventConsumer) Start() error {
	if m.OnChangedRowEvent == nil {
		return errors.New("OnEvent is null")
	}
	m.stopped = false
	go func() {
		for !m.stopped {
			time.Sleep(100 * time.Millisecond)
			ev, _ := m.streamer.GetEvent()
			if ev != nil {

				switch e := ev.Event.(type) {
				case *replication.RotateEvent:
					m.CurrentPosition.LogFile = string(e.NextLogName)
					m.CurrentPosition.Position = int(e.Position)

				case *replication.RowsEvent:
					if changedRowEvent, _ := m.handleRowsEvent(ev); changedRowEvent != nil {
						m.OnChangedRowEvent(changedRowEvent)
						m.CurrentPosition.Position = int(ev.Header.LogPos)
					}
				default:
				}
				m.stateRepository.SaveLastPosition(m.CurrentPosition)
			}
		}
	}()
	return nil
}

func (m *RowEventConsumer) GetPosition() (mysql.Position, error) {
	resultPosition := mysql.Position{}
	lastSavedposition, err := m.stateRepository.GetLastPosition()
	if err != nil {
		return resultPosition, err
	}

	if m.Config.LogPosition != nil {
		resultPosition.Name = m.Config.LogPosition.LogFile
		resultPosition.Pos = uint32(m.Config.LogPosition.Position)
	}

	if m.Config.LogPosition == nil && lastSavedposition != nil {
		resultPosition.Name = lastSavedposition.LogFile
		resultPosition.Pos = uint32(lastSavedposition.Position)
		return resultPosition, nil
	}

	if m.Config.LogPosition == nil && lastSavedposition == nil {
		execRes, err := m.connection.Execute("SHOW MASTER STATUS")
		if err != nil {
			return resultPosition, err
		}
		name, _ := execRes.GetString(0, 0)
		pos, _ := execRes.GetInt(0, 1)
		resultPosition.Name = name
		resultPosition.Pos = uint32(pos)
		return resultPosition, nil
	}

	return resultPosition, nil
}

func (m *RowEventConsumer) verifyBinLogFormat() error {
	if m.syncer == nil {
		return errors.New("Syncer not instantiated")
	}

	execResult, err := m.connection.Execute(`SHOW GLOBAL VARIABLES LIKE 'binlog_format';`)
	if err != nil {
		return err
	}

	value, err := execResult.GetStringByName(0, "Value")
	if err != nil {
		return err
	}

	if value != "ROW" {
		return fmt.Errorf("BinLog format expected: ROW, actual: %s", value)
	}
	return nil
}

func (m *RowEventConsumer) handleRowsEvent(e *replication.BinlogEvent) (*ChangedRowEvent, error) {
	ev, ok := e.Event.(*replication.RowsEvent)

	if !ok {
		return nil, nil
	}

	schemaName := string(ev.Table.Schema)
	tableName := string(ev.Table.Table)
	// Caveat: table may be altered at runtime.
	table, err := m.getTableSchema(schemaName, tableName)
	if err != nil {
		return nil, err
	}

	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = canal.InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = canal.DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = canal.UpdateAction
	default:
		return nil, fmt.Errorf("%s not supported now", e.Header.EventType)
	}

	changedRowEvent := NewChangedRowEvent(table, action, ev.Rows)
	return changedRowEvent, nil
}

func (m *RowEventConsumer) getTableSchema(schemaName string, tableName string) (*schema.Table, error) {
	tableKeyName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if tableSchema, ok := m.schemaCache[tableKeyName]; ok {
		return tableSchema, nil
	}

	tableSchema, err := schema.NewTable(m.connection, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	m.schemaCache[tableKeyName] = tableSchema

	return tableSchema, nil
}

func (m *RowEventConsumer) Close() {
	m.stopped = true
	m.syncer.Close()
	m.connection.Close()
}

func NewRowEventConsumer(config RowEventConsumerConfig, stateRepository BinLogStateRepository) (*RowEventConsumer, error) {
	consumer := &RowEventConsumer{
		Config:          config,
		stateRepository: stateRepository,
	}
	if err := consumer.init(); err != nil {
		return nil, err
	}
	return consumer, nil
}
