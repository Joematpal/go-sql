CREATE TABLE IF NOT EXISTS usersettings (
    userId varchar,
    medatadata map<int,text>,
    createdAt bigint,
    updatedAt bigint,
    createdBy varchar,
    updatedBy varchar, 
    PRIMARY KEY (userId)
)
