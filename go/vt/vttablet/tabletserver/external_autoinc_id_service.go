package tabletserver

import (
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

type ExternalAutoIncIDService interface {
	BindTopoServer(topoServer *topo.Server)
	BindSchemaEngine(schemaEngine *schema.Engine)
	InitializeExternalAutoIncIDService(target *querypb.Target)
	IsExternalAutoIncIDEnabled(keyspace string, tableName string) bool
	GetNextID(tableName string, sequenceFields []*querypb.Field) (*sqltypes.Result, error)
}

type DisabledExternalAutoIncIDService struct{}

func (d *DisabledExternalAutoIncIDService) BindTopoServer(topoServer *topo.Server) {
	// no-op / do nothing
}

func (d *DisabledExternalAutoIncIDService) BindSchemaEngine(schemaEngine *schema.Engine) {
	//  no-op / do nothing
}

func (d *DisabledExternalAutoIncIDService) InitializeExternalAutoIncIDService(target *querypb.Target) {
	//  no-op / do nothing
}

func (d *DisabledExternalAutoIncIDService) IsExternalAutoIncIDEnabled(keyspace string, tableName string) bool {
	return false
}

func (d *DisabledExternalAutoIncIDService) GetNextID(tableName string, sequenceFields []*querypb.Field) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("exteranl auto-increment ID service is not implemented and disabled")
}

var externalAutoIncIDService ExternalAutoIncIDService = &DisabledExternalAutoIncIDService{}

func RegisterExternalAutoIncIDService(thirdPartyExternalAutoIncIDService ExternalAutoIncIDService) {
	externalAutoIncIDService = thirdPartyExternalAutoIncIDService
}
