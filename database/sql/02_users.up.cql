CREATE TABLE IF NOT EXISTS users (
    userId char(20),
    username text,
    email text,
    verified boolean,
    createdAt bigint,
    updatedAt bigint,
    createdBy text,
    updatedBy text, 
    PRIMARY KEY (userId)
)