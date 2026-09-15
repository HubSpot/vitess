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

package tableacl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/tchap/go-patricia/patricia"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/log"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
	"vitess.io/vitess/go/vt/tableacl/acl"
)

// ACLResult embeds an acl.ACL and also tell which table group it belongs to.
type ACLResult struct {
	acl.ACL
	GroupName string
}

type aclEntry struct {
	tableNameOrPrefix string
	groupName         string
	acl               map[Role]acl.ACL
}

type aclEntries []aclEntry

func (aes aclEntries) Len() int {
	return len(aes)
}

func (aes aclEntries) Less(i, j int) bool {
	return aes[i].tableNameOrPrefix < aes[j].tableNameOrPrefix
}

func (aes aclEntries) Swap(i, j int) {
	aes[i], aes[j] = aes[j], aes[i]
}

// mu protects acls and defaultACL.
var mu sync.Mutex

var acls = make(map[string]acl.Factory)

// defaultACL tells the default ACL implementation to use.
var defaultACL string

type tableACL struct {
	// mutex protects entries, overrides, config, and callback
	sync.RWMutex
	entries aclEntries
	config  *tableaclpb.Config

	// overrides are always checked first; if a table doesn't match here it falls through to entries.
	overrides aclEntries

	// callback is executed on successful reload.
	callback func()
	// ACL Factory override for testing
	factory acl.Factory
}

// currentTableACL stores current effective ACL information.
var currentTableACL tableACL

// Init initiates table ACLs.
//
// The config file can be binary-proto-encoded, or json-encoded.
// In the json case, it looks like this:
//
//	{
//	  "table_groups": [
//	    {
//	      "table_names_or_prefixes": ["name1"],
//	      "readers": ["client1"],
//	      "writers": ["client1"],
//	      "admins": ["client1"]
//	    }
//	  ]
//	}
func Init(configFile string, aclCB func()) error {
	return currentTableACL.init(configFile, aclCB)
}

func (tacl *tableACL) init(configFile string, aclCB func()) error {
	tacl.SetCallback(aclCB)
	if configFile == "" {
		return nil
	}
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Errorf("unable to read tableACL config file: %v  Error: %v", configFile, err)
		return err
	}
	if len(data) == 0 {
		return errors.New("tableACL config file is empty")
	}

	config := &tableaclpb.Config{}
	if err := config.UnmarshalVT(data); err != nil {
		// try to parse tableacl as json file
		if jsonErr := json2.UnmarshalPB(data, config); jsonErr != nil {
			log.Errorf("unable to parse tableACL config file as a protobuf or json file.  protobuf err: %v  json err: %v", err, jsonErr)
			return fmt.Errorf("unable to unmarshal Table ACL data: %s", data)
		}
	}
	return tacl.Set(config)
}

func (tacl *tableACL) SetCallback(callback func()) {
	tacl.Lock()
	defer tacl.Unlock()
	tacl.callback = callback
}

// InitFromProto inits table ACLs from a proto.
func InitFromProto(config *tableaclpb.Config) error {
	return currentTableACL.Set(config)
}

// load loads configurations from a proto-defined Config.
// If err is nil, then entries and overrides are guaranteed to be non-nil (though possibly empty).
func load(config *tableaclpb.Config, newACL func([]string) (acl.ACL, error)) (entries aclEntries, overrides aclEntries, err error) {
	if err := ValidateProto(config); err != nil {
		return nil, nil, err
	}
	entries = aclEntries{}
	overrides = aclEntries{}
	for _, group := range config.TableGroups {
		readers, err := newACL(group.Readers)
		if err != nil {
			return nil, nil, err
		}
		writers, err := newACL(group.Writers)
		if err != nil {
			return nil, nil, err
		}
		admins, err := newACL(group.Admins)
		if err != nil {
			return nil, nil, err
		}

		entry := aclEntry{
			groupName: group.Name,
			acl: map[Role]acl.ACL{
				READER: readers,
				WRITER: writers,
				ADMIN:  admins,
			},
		}

		for _, tableNameOrPrefix := range group.TableNamesOrPrefixes {
			entry.tableNameOrPrefix = tableNameOrPrefix
			if group.IsOverride {
				overrides = append(overrides, entry)
			} else {
				entries = append(entries, entry)
			}
		}
	}
	sort.Sort(entries)
	sort.Sort(overrides)
	return entries, overrides, nil
}

func (tacl *tableACL) aclFactory() (acl.Factory, error) {
	if tacl.factory == nil {
		return GetCurrentACLFactory()
	}
	return tacl.factory, nil
}

func (tacl *tableACL) Set(config *tableaclpb.Config) error {
	factory, err := tacl.aclFactory()
	if err != nil {
		return err
	}
	entries, overrides, err := load(config, factory.New)
	if err != nil {
		return err
	}
	tacl.Lock()
	tacl.entries = entries
	tacl.overrides = overrides
	tacl.config = config.CloneVT()
	callback := tacl.callback
	tacl.Unlock()
	if callback != nil {
		callback()
	}
	return nil
}

// Valid returns whether the tableACL is valid.
// Currently it only checks that it has been initialized.
func (tacl *tableACL) Valid() bool {
	tacl.RLock()
	defer tacl.RUnlock()
	return tacl.entries != nil
}

// ValidateProto returns an error if the given proto has problems
// that would cause InitFromProto to fail.
func ValidateProto(config *tableaclpb.Config) (err error) {
	// Maintain separate tries for standard and override entries. A table may overlap between
	// a standard group and an override group, but not within either category.
	trieRegular := patricia.NewTrie()
	trieOverride := patricia.NewTrie()
	for _, group := range config.TableGroups {
		t := trieRegular
		if group.IsOverride {
			t = trieOverride
		}
		for _, name := range group.TableNamesOrPrefixes {
			var prefix patricia.Prefix
			if strings.HasSuffix(name, "%") {
				prefix = []byte(strings.TrimSuffix(name, "%"))
			} else {
				prefix = []byte(name + "\000")
			}
			if bytes.Contains(prefix, []byte("%")) {
				return fmt.Errorf("got: %s, '%%' means this entry is a prefix and should not appear in the middle of name or prefix", name)
			}
			overlapVisitor := func(_ patricia.Prefix, item patricia.Item) error {
				return fmt.Errorf("conflicting entries: %q overlaps with %q", name, item)
			}
			if err := t.VisitSubtree(prefix, overlapVisitor); err != nil {
				return err
			}
			if err := t.VisitPrefixes(prefix, overlapVisitor); err != nil {
				return err
			}
			t.Insert(prefix, name)
		}
	}
	return nil
}

// Authorized returns the list of entities who have the specified role on a table.
func Authorized(table string, role Role) *ACLResult {
	return currentTableACL.Authorized(table, role)
}

// Authorized checks the overrides ACL list first; if there's no matching entry in overrides
// it falls back to the standard entries list.
func (tacl *tableACL) Authorized(table string, role Role) *ACLResult {
	tacl.RLock()
	defer tacl.RUnlock()

	if r := tacl.overrides.checkEntries(table, role); r != nil {
		return r
	}
	if r := tacl.entries.checkEntries(table, role); r != nil {
		return r
	}
	return &ACLResult{
		ACL:       acl.DenyAllACL{},
		GroupName: "",
	}
}

// checkEntries returns the matching ACLResult for the given table and role, or nil if no
// entry in the sorted slice matches. A nil return signals the caller to fall through to the
// next ACL layer rather than denying.
func (entries aclEntries) checkEntries(table string, role Role) *ACLResult {
	start := 0
	end := len(entries)
	for start < end {
		mid := start + (end-start)/2
		val := entries[mid].tableNameOrPrefix
		if table == val || (strings.HasSuffix(val, "%") && strings.HasPrefix(table, val[:len(val)-1])) {
			acl, ok := entries[mid].acl[role]
			if ok {
				return &ACLResult{
					ACL:       acl,
					GroupName: entries[mid].groupName,
				}
			}
			break
		} else if table < val {
			end = mid
		} else {
			start = mid + 1
		}
	}
	return nil
}

// GetCurrentConfig returns a copy of current tableacl configuration.
func GetCurrentConfig() *tableaclpb.Config {
	return currentTableACL.Config()
}

func (tacl *tableACL) Config() *tableaclpb.Config {
	tacl.RLock()
	defer tacl.RUnlock()
	return tacl.config.CloneVT()
}

// Register registers an AclFactory.
func Register(name string, factory acl.Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := acls[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	acls[name] = factory
}

// SetDefaultACL sets the default ACL implementation.
func SetDefaultACL(name string) {
	mu.Lock()
	defer mu.Unlock()
	defaultACL = name
}

// GetCurrentACLFactory returns current table acl implementation.
func GetCurrentACLFactory() (acl.Factory, error) {
	mu.Lock()
	defer mu.Unlock()
	if len(acls) == 0 {
		return nil, fmt.Errorf("no AclFactories registered")
	}
	if defaultACL == "" {
		if len(acls) == 1 {
			for _, aclFactory := range acls {
				return aclFactory, nil
			}
		}
		return nil, errors.New("there are more than one AclFactory registered but no default has been given")
	}
	if aclFactory, ok := acls[defaultACL]; ok {
		return aclFactory, nil
	}
	return nil, fmt.Errorf("aclFactory for given default: %s is not found", defaultACL)
}
