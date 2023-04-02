CREATE KEYSPACE spark_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE spark_db;
CREATE TABLE users(profile_id int PRIMARY KEY, user_name text, remaining float, age int);

INSERT INTO users (profile_id, user_name, remaining, age) VALUES( 102103, 'Prashant', 200.00,19);
INSERT INTO users (profile_id, user_name, remaining, age) VALUES( 102104, 'Alisha', 348.32,20);
INSERT INTO users (profile_id, user_name, remaining, age) VALUES( 102105, 'Abdul', 566.00,33);

INSERT INTO users (profile_id, user_name, remaining, age) VALUES();