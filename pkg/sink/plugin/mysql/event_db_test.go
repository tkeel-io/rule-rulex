package mysql

import (
	"context"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"reflect"
	"testing"
)

func TestEventDB_BatchCreate(t *testing.T) {
	type fields struct {
		balance LoadBalance
		dbName  string
		table   string
	}
	type args struct {
		ctx       context.Context
		sqlCreate string
		values    interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "createEvent",
			fields: fields{
				balance: NewLoadBalanceRandom([]*Server{}),
				dbName:  "iot_manage",
				table:   "device_thing_data",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventDB{
				balance: tt.fields.balance,
				dbName:  tt.fields.dbName,
				table:   tt.fields.table,
			}
			if err := e.BatchCreate(tt.args.ctx, tt.args.sqlCreate, tt.args.values); (err != nil) != tt.wantErr {
				t.Errorf("BatchCreate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEventDB_Close(t *testing.T) {
	type fields struct {
		balance LoadBalance
		dbName  string
		table   string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "createEvent",
			fields: fields{
				balance: NewLoadBalanceRandom([]*Server{}),
				dbName:  "iot_manage",
				table:   "device_thing_data",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventDB{
				balance: tt.fields.balance,
				dbName:  tt.fields.dbName,
				table:   tt.fields.table,
			}
			if err := e.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewEventDB(t *testing.T) {
	type args struct {
		dbName string
		table  string
		urls   []string
	}
	tests := []struct {
		name      string
		args      args
		wantEvent sink.DBImpl
		wantErr   bool
	}{
		{
			name: "createEvent",
			args: args{
				urls:   []string{},
				dbName: "iot_manage",
				table:  "device_thing_data",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEvent, err := NewEventDB(tt.args.dbName, tt.args.table, tt.args.urls)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewEventDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotEvent, tt.wantEvent) {
				t.Errorf("NewEventDB() gotEvent = %v, want %v", gotEvent, tt.wantEvent)
			}
		})
	}
}
