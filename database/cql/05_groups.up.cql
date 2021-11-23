CREATE TABLE IF NOT EXISTS groups (
    user_id text,
    group_id text,
    created_at bigint,
    updated_at bigint,
    created_by text,
    updated_by text, 
    PRIMARY KEY (group_id, user_id)
)
