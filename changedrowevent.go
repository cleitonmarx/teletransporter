package main

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/schema"
)

type ChangedRow struct {
	OldValue     map[string]interface{}
	CurrentValue map[string]interface{}
}

type ChangedRowEvent struct {
	Table  *schema.Table
	Action string
	Rows   []ChangedRow
}

func NewChangedRowEvent(table *schema.Table, action string, rows [][]interface{}) *ChangedRowEvent {
	var lenRows int
	if lenRows = len(rows); action == canal.UpdateAction {
		lenRows = len(rows) / 2
	}

	event := new(ChangedRowEvent)
	event.Table = table
	event.Action = action
	event.Rows = make([]ChangedRow, lenRows)

	idxUpdateAction := 0

	for idxRows, column := range rows {
		rowValues := make(map[string]interface{})
		for idxColumn, value := range column {
			rowValues[table.Columns[idxColumn].Name] = value
		}
		if action == canal.UpdateAction {
			if idxRows%2 == 0 {
				event.Rows[idxUpdateAction].OldValue = rowValues
			} else {
				event.Rows[idxUpdateAction].CurrentValue = rowValues
				idxUpdateAction++
			}
		} else if action == canal.DeleteAction {
			event.Rows[idxRows].OldValue = rowValues
		} else {
			event.Rows[idxRows].CurrentValue = rowValues
		}
	}
	return event
}
