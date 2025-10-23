package tabletserver

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Register Tablet Server Function Sets link to TabletServer for InitializeVTickets to use later
// InitializeVTickets is called when the tablet becomes primary in state_manager.go
// IsVTicketsEnabled is called to check if the table is using vTickets in query_executor.go
// GetNextVTicketID is called to get the next vTicket ID in query_executor.go

type VTicketsService interface {
	RegisterTabletServer(tabletServer *TabletServer)
	InitializeVTickets(target *querypb.Target)
	IsVTicketsEnabled(keyspace string, tableName string) bool
	GetNextVTicketID(tableName string, sequenceFields []*querypb.Field) (*sqltypes.Result, error)
}

type VTicketsServiceUninitialized struct{}

func (d *VTicketsServiceUninitialized) RegisterTabletServer(tabletServer *TabletServer) {
	// vtickets not enabled do nothing
	log.Infof("VTICKETS: VTicketsServiceUninitialized.RegisterTabletServer: this should not be called...")
}

func (d *VTicketsServiceUninitialized) InitializeVTickets(target *querypb.Target) {
	// vtickets not enabled do nothing
}

func (d *VTicketsServiceUninitialized) IsVTicketsEnabled(keyspace string, tableName string) bool {
	return false
}

func (d *VTicketsServiceUninitialized) GetNextVTicketID(tableName string, sequenceFields []*querypb.Field) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("vTickets Service not enabled or initialized")
}

var vTicketsService VTicketsService = &VTicketsServiceUninitialized{}

func RegisterVTicketsService(thirdPartyVTicketsService VTicketsService) {
	log.Infof("VTICKETS: RegisterVTicketsService: registering third party vTicketsService")
	vTicketsService = thirdPartyVTicketsService
}
