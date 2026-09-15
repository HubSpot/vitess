package tabletmanager

import (
	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

const extraWarning = "This will eventually fail as invalid MySQL 5.6 SID \"\" when setting up tablet replication"

// hubspotUpgradeFromV14Fix is a fix for the issue where the primary is running an older Vitess version
// (e.g., Vitess 14) where PrimaryStatus proto doesn't include server_uuid.
// Fall back to FullStatus which has ServerUuid in all versions.
func hubspotUpgradeFromV14Fix(ctx context.Context, tm *TabletManager, serverUuid string, parent *topo.TabletInfo) string {
	// if serverUuid is not empty, return it immediately as this fix isn't needed
	if serverUuid != "" {
		return serverUuid
	}
	// If ServerUuid is empty, the primary may be running an older Vitess version
	// (e.g., Vitess 14) where PrimaryStatus proto doesn't include server_uuid.
	// Fall back to FullStatus which has ServerUuid in all versions.
	fullStatus, err := tm.tmc.FullStatus(ctx, parent.Tablet)
	if err != nil {
		log.Errorf("HubSpot Fix: Failed to get FullStatus: %v; %s", err, extraWarning)
		return ""
	}
	if fullStatus.ServerUuid == "" {
		log.Errorf("HubSpot Fix: ServerUuid was not available in PrimaryStatus or FullStatus; %s", extraWarning)
		return ""
	}
	log.Warningf("HubSpot Fix: Detected an older Vitess version, falling back to FullStatus for ServerUuid: %s", fullStatus.ServerUuid)
	return fullStatus.ServerUuid
}
