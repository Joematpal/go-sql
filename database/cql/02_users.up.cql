CREATE TABLE IF NOT EXISTS users (
    user_id varchar,
    username text,
    email text,
    verified boolean,
    created_at bigint,
    updated_at bigint,
    created_by varchar,
    updated_by varchar, 
    PRIMARY KEY (user_id)
)
