package test_users

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
	UserId    string            `json:"userId"`
	Metadata  map[string]string `json:"metadata"`
	CreatedAt int64             `json:"createdAt,omitempty"`
	UpdatedAt int64             `json:"updatedAt,omitempty"`
	CreatedBy string            `json:"createdBy,omitempty"`
	UpdatedBy string            `json:"updatedBy,omitempty"`
}

type UserGroup struct {
	GroupID   string `json:"groupId"`
	UserID    string `json:"userId"`
	CreatedAt int64  `json:"createdAt,omitempty"`
	UpdatedAt int64  `json:"updatedAt,omitempty"`
	CreatedBy string `json:"createdBy,omitempty"`
	UpdatedBy string `json:"updatedBy,omitempty"`
}
