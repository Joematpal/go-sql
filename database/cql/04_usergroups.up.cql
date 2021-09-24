CREATE TABLE IF NOT EXISTS usergroups (
    userId text,
    groupId text,
    createdAt bigint,
    updatedAt bigint,
    createdBy text,
    updatedBy text, 
    PRIMARY KEY (userId, groupId)
)
