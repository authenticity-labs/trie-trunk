CREATE TABLE pg_database(datname TEXT, datistemplate INTEGER);
CREATE TABLE pg_namespace(oid INTEGER PRIMARY KEY, nspname TEXT);
INSERT INTO pg_namespace VALUES (0,'pg_catalog');
CREATE TABLE pg_class(oid INTEGER PRIMARY KEY, relname TEXT, relnamespace INTEGER, relkind TEXT, reltuples INTEGER);
CREATE TABLE pg_attribute(attrelid INTEGER, attnum INTEGER, attname TEXT, atttypid INTEGER, attisdropped INTEGER, attnotnull INTEGER, atttypmod INTEGER);
CREATE TABLE pg_type(oid INTEGER PRIMARY KEY, typname TEXT, typnamespace INTEGER, typrelid INTEGER, typelem INTEGER, typarray INTEGER);
-- Trunk event relations declare no SQL foreign keys. Key arrays use one-based PostgreSQL ordinals.
CREATE TABLE pg_constraint(oid INTEGER, conname TEXT, contype TEXT, conrelid INTEGER, confrelid INTEGER, conkey TEXT, confkey TEXT, confupdtype TEXT, confdeltype TEXT);
CREATE TABLE pg_inherits(inhrelid INTEGER, inhparent INTEGER);
CREATE TABLE pg_enum(enumtypid INTEGER, enumlabel TEXT, enumsortorder INTEGER);
CREATE TABLE pg_proc(oid INTEGER, proname TEXT);
CREATE VIEW schemata AS SELECT nspname AS schema_name FROM pg_namespace WHERE oid <> 0;
CREATE VIEW tables AS SELECT nspname AS table_schema, relname AS table_name, 'BASE TABLE' AS table_type FROM pg_class JOIN pg_namespace ON relnamespace=pg_namespace.oid;
CREATE VIEW columns AS SELECT nspname AS table_schema, relname AS table_name, attname AS column_name,
    attnum AS ordinal_position, 'YES' AS is_nullable, 'NEVER' AS is_generated, NULL AS column_default,
    typname AS udt_name, 'pg_catalog' AS udt_schema, CASE typname WHEN 'timestamptz' THEN 'timestamp with time zone' ELSE typname END AS data_type,
    NULL AS character_maximum_length, NULL AS numeric_precision, NULL AS numeric_scale, NULL AS datetime_precision
    FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_type t ON t.oid=a.atttypid;
CREATE VIEW views AS SELECT * FROM tables WHERE 0;
CREATE TABLE routines(specific_name TEXT, routine_schema TEXT, routine_name TEXT, routine_type TEXT, data_type TEXT);
CREATE TABLE parameters(specific_schema TEXT,specific_name TEXT,parameter_name TEXT,character_maximum_length INTEGER,data_type TEXT,parameter_mode TEXT,ordinal_position INTEGER);
CREATE TABLE pg_index(indrelid INTEGER, indkey INTEGER, indisprimary INTEGER);
