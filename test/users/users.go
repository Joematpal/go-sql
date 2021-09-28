package test_users

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"
)

type User struct {
	UserID    string `json:"userId,omitempty"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Verified  bool   `json:"verified,omitempty"`
	CreatedAt int64  `json:"createdAt,omitempty"`
	UpdatedAt int64  `json:"updatedAt,omitempty"`
	CreatedBy string `json:"createdBy,omitempty"`
	UpdatedBy string `json:"updatedBy,omitempty"`
}

type UserSettings struct {
	UserId    string            `json:"user_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Scope     *Scope            `json:"scope,omitempty"`
	CreatedAt int64             `json:"created_at,omitempty"`
	UpdatedAt int64             `json:"updated_at,omitempty"`
	CreatedBy string            `json:"created_by,omitempty"`
	UpdatedBy string            `json:"updated_by,omitempty"`
}

type UserGroup struct {
	GroupID   string `json:"groupId"`
	UserID    string `json:"userId"`
	CreatedAt int64  `json:"createdAt,omitempty"`
	UpdatedAt int64  `json:"updatedAt,omitempty"`
	CreatedBy string `json:"createdBy,omitempty"`
	UpdatedBy string `json:"updatedBy,omitempty"`
}
type ScopeOperation int

const (
	_ ScopeOperation = iota
	ScopeOperation_Read
)

func (so ScopeOperation) String() string {
	switch so {
	case ScopeOperation_Read:
		return "READ"
	}

	return ""
}

func ParseScopeOperation(s string) (ScopeOperation, error) {
	switch s {
	case "READ":
		return 1, nil
	}
	return 0, errors.New("enum not found")
}

type Scope struct {
	Domain    string         `json:"domain,omitempty"`
	Operation ScopeOperation `json:"operation,omitempty"`
}

func (s Scope) MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error) {
	switch name {
	case "domain":
		return []byte(s.Domain), nil
	case "operation":
		return []byte(s.Operation.String()), nil

	default:
		return nil, fmt.Errorf("unknown column for position: %q", name)
	}
}

func (s *Scope) UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error {
	switch name {
	case "domain":
		s.Domain = string(data)
		return nil
	case "operation":
		var err error
		s.Operation, err = ParseScopeOperation(string(data))
		return err

	default:
		return fmt.Errorf("unknown column for position: %q", name)
	}
}
