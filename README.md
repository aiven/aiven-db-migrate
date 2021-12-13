# aiven-db-migrate

Aiven database migration tool. This tool is meant for easy migration of databases from some database service
provider, such AWS RDS, or on premises data center, to [Aiven Database as a Service](https://aiven.io/).
However, it's not limited for Aiven services and it might be useful as a generic database migration tool.

Usually database service providers, such as Aiven, AWS RDS, and alike, don't allow superuser/root access.
Instead the service's master/admin user is granted permissions for the most common DBA tasks, see e.g.
https://help.aiven.io/en/articles/489557-postgresql-superuser-access.
In addition, service provider's web console/API can be used for performing some DBA tasks requiring more privileges than
granted for the master/admin user. However, the missing superuser access makes some existing database migrating tools,
such as `pg_dumpall`, not useful when migrating database to/from service provider. 

Currently this tool supports only PostgreSQL but we aim to add support for other databases, such as MySQL.

Requires Python 3.5 or newer.

## Usage

Running library module:
```
$ python3 -m aiven_db_migrate.migrate -h
Available commands: pg
```

Installing in virtualenv:
```
$ python3 -m venv venv
$ . venv/bin/activate
$ ## Run make to set the proper version
$ make
$ pip install .
```

This installs console scripts which have the same interface as the library module:
 * `pg_migrate`: PostgreSQL migration

## PostgreSQL

Requirements:
 * `pg_dump`: from any PostgreSQL version between the source and target versions
 * `psql`: any modern version should work

Run library module:
```
$ python3 -m aiven_db_migrate.migrate pg -h
```
or, if installed:
```
$ pg_migrate -h
```

Migrating is supported to the same or newer PostgreSQL version starting from PostgreSQL 9.5 to PostgreSQL 13.
Migrating to older version is not supported.

Supports regular data dump (`pg_dump`) and [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) (PostgreSQL 10 or newer).
In case that logical replication is not available or privileges/requirements are missing migrating falls back to
data dump.

### CLI example

Migrating from AWS RDS to Aiven for PostgreSQL. Logical replication is enabled in source AWS RDS PostgreSQL
server and `aiven-extras` extension is installed in target database.

```
$ pg_migrate -s "postgres://postgres:<password>@jappja-pg1.chfhzaircbpb.eu-west-1.rds.amazonaws.com:5432/defaultdb" -t "postgres://avnadmin:<password>@pg1-test-jappja-test.avns.net:26192/defaultdb?sslmode=require"
...

Roles:
  rolname: 'rdsadmin', rolpassword: None, status: 'failed', message: 'must be superuser to create superusers'
  rolname: 'rds_password', rolpassword: None, status: 'created', message: 'role created'
  rolname: 'rds_superuser', rolpassword: None, status: 'created', message: 'role created'
  rolname: 'test_user1', rolpassword: 'placeholder_kfbqrvmdhgrpgpvy', status: 'created', message: 'role created'
  rolname: 'rds_ad', rolpassword: None, status: 'created', message: 'role created'
  rolname: 'rds_iam', rolpassword: None, status: 'created', message: 'role created'
  rolname: 'rds_replication', rolpassword: None, status: 'created', message: 'role created'
  rolname: 'rdsrepladmin', rolpassword: None, status: 'failed', message: 'must be superuser to create replication users'
  rolname: 'postgres', rolpassword: None, status: 'exists', message: 'role already exists'
  rolname: 'test_user2', rolpassword: None, status: 'created', message: 'role created'

Databases:
  dbaname: 'rdsadmin', method: None, status: 'failed', message: 'FATAL:  pg_hba.conf rejects connection for host "80.220.195.174", user "postgres", database "rdsadmin", SSL on\nFATAL:  pg_hba.conf rejects connection for host "80.220.195.174", user "postgres", database "rdsadmin", SSL off\n'
  dbaname: 'defaultdb', method: 'replication', status: 'running', message: 'migrated to existing database'
```

By default logical replication is left running and the created pub/sub objects need to be cleaned up once workloads have been
moved to the new server. Objects created by this tool are named like `aiven_db_migrate_<dbname>_<sub|pub|slot>`.

Starting from the target (using `aiven-extras` extension), get first the subscription name:
```
defaultdb= > SELECT * FROM aiven_extras.pg_list_all_subscriptions();
```
and then drop it:
```
defaultdb= > SELECT * FROM aiven_extras.pg_drop_subscription('aiven_db_migrate_defaultdb_sub');
```

Note that with `aiven-extras` dropping subscription in target also drops replication slot in source (`dblink`).

In the source get first the publication name:
```
defaultdb=> SELECT * FROM pg_publication;
```
and then drop it:
```
defaultdb=> DROP PUBLICATION aiven_db_migrate_defaultdb_pub;
```

In case that `aiven-extras` is not used clean up replication slot too:
```
defaultdb=> SELECT * FROM pg_replication_slots;
defaultdb=> SELECT * FROM pg_drop_replication_slot('aiven_db_migrate_defaultdb_slot');
```

Using `--max-replication-lag` waits until replication lag in bytes is less than/equal to given max replication lag. This
can be used together with `--stop-replication` to clean up all created pub/sub objects when replication is done.

With `--validate` only best effort validation is run. This checks e.g. PL/pgSQL languages, extensions etc. installed
in source are also installed/available in target.

Use `--no-replicate-extension-tables` to skip extension tables.  By default it attempts to replicate all extension tables during logical replication.

With `--force-method` you can specify if you wish to use either replication or dump method. Otherwise the most suitable method is chosen automatically.

### API example

Migrating from AWS RDS to Aiven for PostgreSQL. Logical replication is enabled in source AWS RDS PostgreSQL
server but `aiven-extras` extension is not installed in target database so migrating falls back to data dump.

```
>>> from aiven.migrate import PGMigrate, PGMigrateResult
>>> pg_mig = PGMigrate(source_conn_info="postgres://postgres:<password>@jappja-pg1.chfhzaircbpb.eu-west-1.rds.amazonaws.com:5432/defaultdb", target_conn_info="postgres://avnadmin:<password>@pg2-test-jappja-test.avns.net:26192/defaultdb?sslmode=require")
>>> result: PGMigrateResult = pg_mig.migrate()
...
Logical replication failed with error: 'must be superuser to create subscriptions', fallback to dump
>>> result
PGMigrateResult(pg_databases={'rdsadmin': {'dbname': 'rdsadmin', 'message': 'FATAL:  pg_hba.conf rejects connection for host "80.220.195.174", user "postgres", database "rdsadmin", SSL on\nFATAL:  pg_hba.conf rejects connection for host "80.220.195.174", user "postgres", database "rdsadmin", SSL off\n', 'method': None, 'status': 'failed'}, 'defaultdb': {'dbname': 'defaultdb', 'message': 'migrated to existing database', 'method': 'dump', 'status': 'done'}}, pg_roles={'rdsadmin': {'message': 'must be superuser to create superusers', 'rolname': 'rdsadmin', 'rolpassword': None, 'status': 'failed'}, 'rds_password': {'message': 'role created', 'rolname': 'rds_password', 'rolpassword': None, 'status': 'created'}, 'rds_superuser': {'message': 'role created', 'rolname': 'rds_superuser', 'rolpassword': None, 'status': 'created'}, 'test_user1': {'message': 'role created', 'rolname': 'test_user1', 'rolpassword': 'placeholder_qkdryldfsrdaocio', 'status': 'created'}, 'rds_ad': {'message': 'role created', 'rolname': 'rds_ad', 'rolpassword': None, 'status': 'created'}, 'rds_iam': {'message': 'role created', 'rolname': 'rds_iam', 'rolpassword': None, 'status': 'created'}, 'rds_replication': {'message': 'role created', 'rolname': 'rds_replication', 'rolpassword': None, 'status': 'created'}, 'rdsrepladmin': {'message': 'must be superuser to create replication users', 'rolname': 'rdsrepladmin', 'rolpassword': None, 'status': 'failed'}, 'postgres': {'message': 'role already exists', 'rolname': 'postgres', 'rolpassword': None, 'status': 'exists'}, 'test_user2': {'message': 'role created', 'rolname': 'test_user2', 'rolpassword': None, 'status': 'created'}})
```

### Logical replication
 * requires PostgreSQL 10 or newer
 * `wal_level` needs to be `logical`
 * currently supports only FOR ALL TABLES publication in source
 * [aiven-extras](https://github.com/aiven/aiven-extras) extension installed in both source and target database, or
 * superuser or superuser-like privileges, such as `rds_replication` role in AWS RDS, in both source and target
 * [AWS RDS additional settings/info](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication)

### Schemas
 * schemas are migrated without object ownership; the user used for migration is given all object ownership
 * NOTE: schema changes break logical replication

### Roles
 * roles with `LOGIN` attribute are migrated using placeholder passwords: `placeholder_<16 char random string>`
 * migrating superuser or replication roles requires superuser privileges

### Extensions
 * requires whitelisting the extension in target with [pgextwlist](https://github.com/dimitri/pgextwlist), or
 * superuser or superuser-like privileges
 * [Aiven for PostgreSQL supported extensions](https://help.aiven.io/en/articles/489561-supported-postgresql-extensions)

## Development

Install build depends (Fedora):
```
$ make build-dep-fedora
```

Style checks:
```
$ make validate-style
```

Fix style errors with:
```
$ make isort
$ make yapf
```

Static checks (`flake8`, `pylint` and `mypy`):
```
$ make static-checks
```

Tests (`pytest`):
```
$ make test
```

Running whole test set takes time since all supported migration paths are tested. During development it's usually enough
to run tests only for a certain PostgreSQL version, e.g.:
```
$ PG_VERSION="12" make test
```

It's also possible to test migration from one PostgreSQL version to another, e.g.:
```
$ PG_SOURCE_VERSION="10" PG_TARGET_VERSION="12" make test
```

Test set can be targeted even further by invoking `pytest`, e.g.:
```
$ PG_SOURCE_VERSION="10" PG_TARGET_VERSION="12" python3 -m pytest -s test/test_pg_migrate.py::Test_PGMigrate::test_migrate
```

# TODO

 * JSON output with CLI (for automation)
   * Hard to make pg_dump silent for outputting JSON to stdout
   * Output json to file instead?
 * More options
   * --dump-only, --repl-only
   * --include-databases, --exclude-databases
   * --include-tables, --exclude-tables
   * --role-passwords (role/passwords file for creating roles with real passwords instead of placeholders)
 * More tests
   * Notably error/corner cases
 * Schema changes break logical replication
   * While logical replication is running dump schema periodically and check if has changed,
     e.g. by calculating hash of the schema dump
   * How to continue if schema has changed? Stop replication, dump schema and restart replication?
 * Proper README + API doc
 * RPM build recipe for aiven-core/prune integration
 * Test automation: Jenkins/Github Actions
