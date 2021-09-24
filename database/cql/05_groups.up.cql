CREATE TABLE IF NOT EXISTS groups (
    userId text,
    groupId text,
    createdAt bigint,
    updatedAt bigint,
    createdBy text,
    updatedBy text, 
    PRIMARY KEY (groupId, userId)
)
