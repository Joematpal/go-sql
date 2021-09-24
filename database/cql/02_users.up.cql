CREATE TABLE IF NOT EXISTS users (
    userId varchar,
    username text,
    email text,
    verified boolean,
    createdAt bigint,
    updatedAt bigint,
    createdBy varchar,
    updatedBy varchar, 
    PRIMARY KEY (userId)
)
