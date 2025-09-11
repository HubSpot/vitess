/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletserver

import (
	"vitess.io/vitess/go/thirdparty/hubspot/vtickets"
)

func init() {
	// Register the vtickets client initialization function
	// This avoids circular imports by registering at init time
	RegisterFunctions = append(RegisterFunctions, func(controller Controller) {
		vtickets.RegisterVTicketsClient(controller)
	})

	// Set up the bridge function for sequence generator registration
	vtickets.SetSequenceGeneratorRegistrationFunc(func(generator *vtickets.SequenceGenerator) {
		RegisterVTicketSequenceGenerator(generator)
	})

	// TODO: Set up VSchema population bridge function
	// This would be used to populate VTickets configuration from VSchema AutoIncrement
	// For now, VTickets configuration should be set programmatically in sequence tables
}
