# pglite 

```
postgres + sqlite = pglite
```

pglite is a server which implements Postgres wire protocol and uses SQLite as its storage engine. Similar to [postlite](https://github.com/benbjohnson/postlite) but it has a lot more comments explaining the implementation :)


# Why
Mainly to learn about Postgres wire protocol. The code has ample of comments. It may help anyone who is looking to understand/implement Postgres wire protocol. Refer https://viggy28.dev/article/introduction-to-postgres-wire-protocol-in-go/

# Install

```
go build .
```

# Run

```
./pglite --datadir prod.db
```

# How to use

Use `psql` or any of your favorite postgres client to connect

```
psql -h localhost

viggy28@dev ~ % psql -h localhost
psql (14.7 (Homebrew), server 0.0.0)
Type "help" for help.

viggy28=> CREATE TABLE emp (id integer);
CREATE TABLE

viggy28=> insert into emp values(1);
INSERT 0 1

viggy28=> select * from emp;
 id
----
 1
(1 row)

viggy28=> insert into emp values(99);
INSERT 0 1
viggy28=>
viggy28=> select * from emp;
 id
----
 1
 99
(2 rows)

viggy28=>
viggy28=> delete from emp;
DELETE 2
viggy28=>
viggy28=> select * from emp;
 id
----
(0 rows)

viggy28=>

```