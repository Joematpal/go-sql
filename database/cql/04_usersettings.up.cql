CREATE TABLE IF NOT EXISTS user_settings (
    user_id varchar,
    metadata map<text,text>,
    scope scope,
    created_at bigint,
    updated_at bigint,
    created_by varchar,
    updated_by varchar, 
    PRIMARY KEY (user_id)
)
