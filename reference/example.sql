CREATE TABLE IF NOT EXISTS schema.table(
    column1 varchar(256),
    column2 varchar(256),
    column3 varchar(256)
    )
    
DELETE FROM schema.table;

COPY schem.table FROM 's3://bucket/data_file.csv' CREDENTIALS 'aws_acces_key_id={key_id};aws_secret_access_key={secret_key}' CSV;

GRANT SELECT ON schema.table TO username;
GRANT SELECT ON schema.table TO GROUP usergroup;