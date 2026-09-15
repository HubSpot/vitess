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
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/tableacl/acl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
)

type fakeACLFactory struct{}

func (factory *fakeACLFactory) New(entries []string) (acl.ACL, error) {
	return nil, errors.New("unable to create a new ACL")
}

func TestInitWithInvalidFilePath(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	if err := tacl.init("/invalid_file_path", func() {}); err == nil {
		t.Fatalf("init should fail for an invalid config file path")
	}
}

var aclJSON = `{
  "table_groups": [
    {
      "name": "group01",
      "table_names_or_prefixes": ["test_table"],
      "readers": ["vt"],
      "writers": ["vt"]
    }
  ]
}`

func TestInitWithValidConfig(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	f, err := os.CreateTemp("", "tableacl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, aclJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tacl.init(f.Name(), func() {}); err != nil {
		t.Fatal(err)
	}
}

var overrideAclJSON = `{
  "table_groups": [
    {
      "name": "group01",
      "table_names_or_prefixes": ["%"],
      "readers": ["vt"],
      "writers": ["vt"]
    },
    {
      "name": "override",
      "table_names_or_prefixes": ["test_table"],
      "readers": ["test"],
      "is_override": true
    }
  ]
}`

func TestInitWithValidOverrideConfig(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	f, err := os.CreateTemp("", "tableacl-override")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, overrideAclJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tacl.init(f.Name(), func() {}); err != nil {
		t.Fatal(err)
	}
}

var overlappingOverrideAclJSON = `{
  "table_groups": [
    {
      "name": "override_a",
      "table_names_or_prefixes": ["test_table%"],
      "readers": ["test"],
      "is_override": true
    },
    {
      "name": "override_b",
      "table_names_or_prefixes": ["test_table_2"],
      "readers": ["test"],
      "is_override": true
    }
  ]
}`

func TestInitWithOverlappingOverrideConfig(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	f, err := os.CreateTemp("", "tableacl-override-invalid")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, overlappingOverrideAclJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tacl.init(f.Name(), func() {}); err == nil {
		t.Fatal("init should fail because two override entries overlap")
	}
}

func TestInitWithEmptyConfig(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	f, err := os.CreateTemp("", "tableacl")
	require.NoError(t, err)

	defer os.Remove(f.Name())
	err = f.Close()
	require.NoError(t, err)

	err = tacl.init(f.Name(), func() {})
	require.Error(t, err)
}

func TestInitFromProto(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	readerACL := tacl.Authorized("my_test_table", READER)
	want := &ACLResult{ACL: acl.DenyAllACL{}, GroupName: ""}
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("tableacl has not been initialized, got: %v, want: %v", readerACL, want)
	}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
		}},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("tableacl init should succeed, but got error: %v", err)
	}
	if got := tacl.Config(); !proto.Equal(got, config) {
		t.Fatalf("GetCurrentConfig() = %v, want: %v", got, config)
	}
	readerACL = tacl.Authorized("unknown_table", READER)
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("there is no config for unknown_table, should deny by default")
	}
	readerACL = tacl.Authorized("test_table", READER)
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "vt"}) {
		t.Fatalf("user: vt should have reader permission to table: test_table")
	}
}

func TestTableACLValidateConfig(t *testing.T) {
	tests := []struct {
		names []string
		valid bool
	}{
		{nil, true},
		{[]string{}, true},
		{[]string{"b"}, true},
		{[]string{"b", "a"}, true},
		{[]string{"b%c"}, false},                    // invalid entry
		{[]string{"aaa", "aaab%", "aaabb"}, false},  // overlapping
		{[]string{"aaa", "aaab", "aaab%"}, false},   // overlapping
		{[]string{"a", "aa%", "aaab%"}, false},      // overlapping
		{[]string{"a", "aa%", "aaab"}, false},       // overlapping
		{[]string{"a", "aa", "aaa%%"}, false},       // invalid entry
		{[]string{"a", "aa", "aa", "aaaaa"}, false}, // duplicate
	}
	for _, test := range tests {
		var groups []*tableaclpb.TableGroupSpec
		for _, name := range test.names {
			groups = append(groups, &tableaclpb.TableGroupSpec{
				TableNamesOrPrefixes: []string{name},
			})
		}
		config := &tableaclpb.Config{TableGroups: groups}
		err := ValidateProto(config)
		if test.valid && err != nil {
			t.Fatalf("ValidateProto(%v) = %v, want nil", config, err)
		} else if !test.valid && err == nil {
			t.Fatalf("ValidateProto(%v) = nil, want error", config)
		}
	}
}

func TestTableACLAuthorize(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"test_music"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
			{
				Name:                 "group02",
				TableNamesOrPrefixes: []string{"test_music_02", "test_video"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u3"},
				Admins:               []string{"u4"},
			},
			{
				Name:                 "group03",
				TableNamesOrPrefixes: []string{"test_other%"},
				Readers:              []string{"u2"},
				Writers:              []string{"u2", "u3"},
				Admins:               []string{"u3"},
			},
			{
				Name:                 "group04",
				TableNamesOrPrefixes: []string{"test_data%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := tacl.Authorized("test_data_any", READER)
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "u1"}) {
		t.Fatalf("user u1 should have reader permission to table test_data_any")
	}
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "u2"}) {
		t.Fatalf("user u2 should have reader permission to table test_data_any")
	}
}

func TestTableACLAuthorizeWithOverride(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u2"},
				Admins:               []string{"u1", "u2"},
			},
			{
				Name:                 "group02-override",
				TableNamesOrPrefixes: []string{"global_table"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1"},
				IsOverride:           true,
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("Set(<override config>) = %v, want: nil", err)
	}

	// Wildcard applies to non-overridden tables: both users can write.
	if !tacl.Authorized("regular_table", WRITER).IsMember(&querypb.VTGateCallerID{Username: "u1"}) {
		t.Fatalf("u1 should have WRITER on regular_table via wildcard")
	}
	if !tacl.Authorized("regular_table", WRITER).IsMember(&querypb.VTGateCallerID{Username: "u2"}) {
		t.Fatalf("u2 should have WRITER on regular_table via wildcard")
	}

	// Override applies to global_table: only u1 keeps WRITER; u2 loses it.
	if !tacl.Authorized("global_table", WRITER).IsMember(&querypb.VTGateCallerID{Username: "u1"}) {
		t.Fatalf("u1 should retain WRITER on global_table via override")
	}
	if tacl.Authorized("global_table", WRITER).IsMember(&querypb.VTGateCallerID{Username: "u2"}) {
		t.Fatalf("u2 should NOT have WRITER on global_table because override strips it")
	}

	// Both users still have READER on global_table per the override's readers list.
	if !tacl.Authorized("global_table", READER).IsMember(&querypb.VTGateCallerID{Username: "u2"}) {
		t.Fatalf("u2 should have READER on global_table via override")
	}
}

func TestFailedToCreateACL(t *testing.T) {
	tacl := tableACL{factory: &fakeACLFactory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
			Writers:              []string{"vt"},
		}},
	}
	if err := tacl.Set(config); err == nil {
		t.Fatalf("tableacl init should fail because fake ACL returns an error")
	}
}

func TestDoubleRegisterTheSameKey(t *testing.T) {
	name := "tableacl-name-TestDoubleRegisterTheSameKey"
	Register(name, &simpleacl.Factory{})
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("the second tableacl register should fail")
		}
	}()
	Register(name, &simpleacl.Factory{})
}

func TestGetCurrentAclFactory(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactory"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	f, err := GetCurrentACLFactory()
	if err != nil {
		t.Errorf("Fail to get current ACL Factory: %v", err)
	}
	if !reflect.DeepEqual(aclFactory, f) {
		t.Fatalf("should return registered acl factory even if default acl is not set.")
	}
	Register(name+"-2", aclFactory)
	_, err = GetCurrentACLFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default is not set")
	}
}

func TestGetCurrentACLFactoryWithWrongDefault(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactoryWithWrongDefault"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	Register(name+"-2", aclFactory)
	SetDefaultACL("wrong_name")
	_, err := GetCurrentACLFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default given does not match any of these.")
	}
}
