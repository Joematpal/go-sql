CREATE TABLE IF NOT EXISTS users (
    user_id char(20),
    username text,
    email text,
    verified boolean,
    created_at bigint,
    updated_at bigint,
    created_by text,
    updated_by text, 
    PRIMARY KEY (user_id)
)