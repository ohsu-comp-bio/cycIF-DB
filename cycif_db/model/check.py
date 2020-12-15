import logging
import os.path
import sys

from migrate.versioning import repository, schema
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from . import mapping

log = logging.getLogger(__name__)

# path relative to galaxy
migrate_repo_dir = os.path.join(os.path.dirname(__file__), 'migrate')
migrate_repository = repository.Repository(migrate_repo_dir)


def create_or_verify_database(url, engine_options={}, auto_migrate=False):
    """
    Check that the database is use-able, possibly creating it if empty (this is
    the only time we automatically create tables, otherwise we force the
    user to do it using the management script so they can create backups).

    1) Empty database --> initialize with latest version and return
    2) Database older than migration support --> fail and require manual update
    3) Database at state where migrate support introduced --> add version
       control information but make no changes (might still require manual
       update)
    4) Database versioned but out of date --> fail with informative message,
       user must run "sh manage_db.sh upgrade"
    """
    # Create the base database if it doesn't yet exist.
    new_database = not database_exists(url)
    if new_database:
        message = "Creating database for URI [%s]" % url
        log.info(message)
        create_database(url)

    # Create engine and metadata
    engine = create_engine(url, **engine_options)

    def migrate():
        try:
            # Declare the database to be under a repository's version control
            db_schema = schema.ControlledSchema.create(engine,
                                                       migrate_repository)
        except Exception:
            # The database is already under version control
            db_schema = schema.ControlledSchema(engine, migrate_repository)
        # Apply all scripts to get to current version
        migrate_to_current_version(engine, db_schema)

    def migrate_from_scratch():
        log.info("Creating new database from scratch, skipping migrations")
        current_version = migrate_repository.version().version
        mapping.init(engine)
        schema.ControlledSchema.create(engine, migrate_repository,
                                       version=current_version)
        db_schema = schema.ControlledSchema(engine, migrate_repository)
        assert db_schema.version == current_version
        migrate()

    if new_database:
        migrate_from_scratch()
        return
    elif auto_migrate:
        migrate()
        return

    # manual migrate
    db_schema = schema.ControlledSchema(engine, migrate_repository)
    if migrate_repository.versions.latest != db_schema.version:
        expect_msg = "Your database has version '%d' but this code expects "\
                     "version '%d'" % (db_schema.version,
                                       migrate_repository.versions.latest)
        instructions = ""
        if db_schema.version > migrate_repository.versions.latest:
            instructions = "To downgrade the database schema you have to "\
                           "checkout the Galaxy version that you were running"\
                           " previously. "
            cmd_msg = "sh manage_db.sh downgrade %d" \
                      % migrate_repository.versions.latest
        else:
            cmd_msg = "sh manage_db.sh upgrade"
        backup_msg = "Please backup your database and then migrate the "\
                     "database schema by running '%s'." % cmd_msg
        raise Exception(f"{expect_msg}. {instructions}{backup_msg}")
    else:
        log.info("At database version %d" % db_schema.version)


def migrate_to_current_version(engine, schema):
    # Changes to get to current version
    try:
        changeset = schema.changeset(None)
    except Exception as e:
        log.error("Problem determining migration changeset for engine [%s]"
                  % engine)
        raise e
    for ver, change in changeset:
        nextver = ver + changeset.step
        log.info(f'Migrating {ver} -> {nextver}... ')
        old_stdout = sys.stdout

        class FakeStdout:
            def __init__(self):
                self.buffer = []

            def write(self, s):
                self.buffer.append(s)

            def flush(self):
                pass
        sys.stdout = FakeStdout()
        try:
            schema.runchange(ver, change, changeset.step)
        finally:
            for message in "".join(sys.stdout.buffer).split("\n"):
                log.info(message)
            sys.stdout = old_stdout
