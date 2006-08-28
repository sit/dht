#include <async.h>
#include <db.h>

/* Generate a configuration file for other processes to use */
static bool
dbfe_generate_config (str path, unsigned int cachesize)
{
  strbuf db_config;
  db_config << "# MIT lsd db configuration\n\n";

  db_config << "set_flags DB_AUTO_COMMIT\n";
  db_config << "set_flags DB_TXN_WRITE_NOSYNC\n";

  // clean up old log files not available in old versions
#if ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR >= 2))
  db_config << "set_flags DB_LOG_AUTOREMOVE\n";
#endif
  db_config << "# create " << cachesize << " KB cache\n";
  db_config << "set_cachesize 0 " << cachesize * 1024 << " 1\n";
  
#if 0
  /* This should be the default */
  char cpath[MAXPATHLEN];
  getcwd(cpath, MAXPATHLEN);
  db_config << "set_data_dir " << cpath << "\n";
  db_config << "set_lg_dir " << cpath << "\n";
  db_config << "set_tmp_dir " << cpath << "\n";
#endif /* 0 */

  // Write out the configuration file
  return str2file (path, db_config);
}

int
dbfe_initialize_dbenv (DB_ENV **dbep, str filename, bool join, unsigned int cachesize = 1024)
{
  int r (-1);

  r = mkdir (filename, 0755);
  if (r < 0 && errno != EEXIST) {
    fatal ("Couldn't mkdir for database %s: %m", filename.cstr ());
  }

  r = db_env_create (dbep, 0);
  if (r) return r;

  DB_ENV *dbe = *dbep;

  // Enable verbose dead lock detection.
  dbe->set_verbose (dbe, DB_VERB_DEADLOCK, 1);
  dbe->set_verbose (dbe, DB_VERB_WAITSFOR, 1);
  dbe->set_lk_detect (dbe, DB_LOCK_DEFAULT);

  dbe->set_errfile (dbe, stderr);

  if (!join) {
    // Force the latest parameters 
    strbuf db_config_path ("%s/DB_CONFIG", filename.cstr ());
    dbfe_generate_config (db_config_path, cachesize);

    // We use all the fixings
    r = dbe->open (dbe, filename, DB_CREATE |
	DB_INIT_MPOOL | 
	DB_INIT_LOCK |
	DB_INIT_LOG |
	DB_INIT_TXN |
	DB_RECOVER , 0);
  } else {
    r = dbe->open (dbe, filename, DB_JOINENV, 0);
  }

  return r;
}

int
dbfe_opendb (DB_ENV *dbe, DB **dbp, str filename, int flags, int mode = 0664, bool dups = false)
{
  int r (-1);
  r = db_create (dbp, dbe, 0);
  if (r) return r;

  DB *db = *dbp;
  db->set_pagesize (db, 16 * 1024);

  /* Secondary databases, for example, require duplicates */
  if (dups && (r = db->set_flags (db, DB_DUPSORT)) != 0) {
    (void)db->close(db, 0);
    dbe->err (dbe, r, "db->set_flags: DB_DUP");
    return r;
  }

  /* the below seems to cause the db to grow much larger. */
  // db->set_bt_minkey(db, 60);

#if ((DB_VERSION_MAJOR < 4) || ((DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 1)))
  r = db->open (db, filename.cstr (), NULL, DB_BTREE, flags, mode);
#else
  if (!dbe) {
    r = db->open (db, NULL, filename.cstr (), NULL, DB_BTREE, flags, mode);
  } else {
    // Sleepycat 4.1 and greater force us to open the DB inside a
    // transaction the open suceeds in either case, but if the open
    // isn't surrounded by a transaction, later calls that use a
    // transaction will fail
    DB_TXN *t = NULL;
    r = dbe->txn_begin (dbe, NULL, &t, 0);
    if (r || !t) return r;
    r = db->open (db, t, filename.cstr (), NULL, DB_BTREE, flags, mode);
    r = t->commit (t, 0);
#endif
  }
  return r;
}
