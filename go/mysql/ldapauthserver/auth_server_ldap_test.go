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

package ldapauthserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	ldap "gopkg.in/ldap.v2"
)

type MockLdapClient struct{}

func (mlc *MockLdapClient) Connect(network string, config *ServerConfig) error { return nil }
func (mlc *MockLdapClient) Close()                                             {}
func (mlc *MockLdapClient) Bind(username, password string) error {
	if username != "testuser" || password != "testpass" {
		return fmt.Errorf("invalid credentials: %s, %s", username, password)
	}
	return nil
}
func (mlc *MockLdapClient) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return &ldap.SearchResult{}, nil
}

func TestValidateClearText(t *testing.T) {
	asl := &AuthServerLdap{
		Client:         &MockLdapClient{},
		User:           "testuser",
		Password:       "testpass",
		UserDnPattern:  "%s",
		RefreshSeconds: 1,
	}
	_, err := asl.validate("testuser", "testpass")
	require.NoError(t, err, "AuthServerLdap failed to validate valid credentials. Got: %v", err)

	_, err = asl.validate("invaliduser", "invalidpass")
	require.Error(t, err, "AuthServerLdap validated invalid credentials.")

}

// flakyLdapClient fails the first Connect and then behaves normally, so a test can drive
// update() through a failed refresh followed by a successful one.
type flakyLdapClient struct {
	connectAttempts int
}

func (c *flakyLdapClient) Connect(network string, config *ServerConfig) error {
	c.connectAttempts++
	if c.connectAttempts == 1 {
		return fmt.Errorf("simulated LDAP connect failure")
	}
	return nil
}
func (c *flakyLdapClient) Close() {}
func (c *flakyLdapClient) Bind(username, password string) error { return nil }
func (c *flakyLdapClient) Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return &ldap.SearchResult{
		Entries: []*ldap.Entry{
			{Attributes: []*ldap.EntryAttribute{{Name: "cn", Values: []string{"refreshedgroup"}}}},
		},
	}, nil
}

// A refresh that fails on an LDAP error must still clear the updating latch. Otherwise every
// later update() short-circuits on the updating check and the user's cached groups freeze
// until the process restarts.
func TestFailedRefreshDoesNotFreezeFutureUpdates(t *testing.T) {
	client := &flakyLdapClient{}
	asl := &AuthServerLdap{
		Client:         client,
		User:           "testuser",
		Password:       "testpass",
		UserDnPattern:  "%s",
		RefreshSeconds: 1,
	}
	lud := &LdapUserData{asl: asl, groups: []string{"stalegroup"}, username: "testuser"}

	// First refresh fails at Connect; cached groups are left untouched.
	lud.update()
	require.Equal(t, []string{"stalegroup"}, lud.groups, "failed refresh should not change cached groups")

	// The failed refresh must not have latched updating=true: a second refresh has to succeed.
	lud.update()
	require.Equal(t, []string{"refreshedgroup"}, lud.groups, "a refresh after an earlier failure must succeed, but the updating latch was left set")
}
