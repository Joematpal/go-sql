CREATE TABLE IF NOT EXISTS user_groups (
    user_id text,
    group_id text,
    created_at bigint,
    updated_at bigint,
    created_by text,
    updated_by text, 
    PRIMARY KEY (user_id, group_id)
)
