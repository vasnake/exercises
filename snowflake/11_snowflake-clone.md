# zero copy clone https://youtu.be/EQ44K5GfgDw?t=4318

`create or replace table qa_customer clone prod_customer;`
quick and easy

docs

https://docs.snowflake.com/en/sql-reference/sql/create-clone
> for creating zero-copy clones of databases, schemas, and tables.

https://docs.snowflake.com/en/user-guide/tables-storage-considerations#label-cloning-tables
> Snowflake’s zero-copy cloning feature provides a convenient way to quickly take a “snapshot” of any table, schema, or database and create a derived copy of that object which initially shares the underlying storage. This can be extremely useful for creating instant backups that do not incur any additional costs (until changes are made to the cloned object).

Clone = zero copy clone: just a snapshot.

# clone vs snapshot https://youtu.be/EQ44K5GfgDw?t=4453

They are the same, clone is done using snapshot technique.
Changes made to V1 or V2 after cloning: don't affect other copy.

# schema level cloneable objects https://youtu.be/EQ44K5GfgDw?t=4561

`create database target_db clone source_db;`
db roles and grants not cloned.
What cloned: schema's:
- roles and grants
- tables
- file formats
- sequences
- named external stages
- pipes
- streams
- tasks

Cloneable:
database, table, schama, stream;
stage, file_format, sequence, tast

transient? temp?

next: simple table cloning https://youtu.be/EQ44K5GfgDw?t=4676
