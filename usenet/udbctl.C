#include <aios.h>
#include <dbfe.h>
#include <parseopt.h>
#include <group.h>

ptr<dbfe> group_db, header_db;

/* Much of the structure and code here is taken from sfskey.C which is GPL2'd.
 * See http://www.fs.net/ 
 * See also lsd/lsdctl.C */

void udbctl_help (int argc, char *argv[]);
void udbctl_addgroup (int argc, char *argv[]);
void udbctl_listgroups (int argc, char *argv[]);
void udbctl_listheaders (int argc, char *argv[]);

struct modevec {
  const char *name;
  void (*fn) (int argc, char **argv);
  const char *usage;
};
const modevec modes[] = {
  { "help", udbctl_help, "help" },
  { "addgroup", udbctl_addgroup, "addgroup groupname" },
  { "listgroups", udbctl_listgroups, "listgroups" },
  { "listheaders", udbctl_listheaders, "listheaders" },
  { NULL, NULL, NULL }
};

static const modevec *udbctl_mode;

void
usage (void)
{
  warnx << "usage: " << progname << " [-d directory] ";
  if (udbctl_mode && udbctl_mode->usage)
    warnx << udbctl_mode->usage << "\n";
  else
    warnx << "command [args]\n";
  exit (1);
}

/**************************************/
/* The commands that do the real work */
/**************************************/

void
udbctl_help (int argc, char *argv[])
{
  strbuf msg;
  msg << "usage: " << progname << " [-d directory] command [args]\n";
  for (const modevec *mp = modes; mp->name; mp++)
    if (mp->usage)
      msg << "	 " << progname << " " << mp->usage << "\n";
  make_sync (1);
  msg.tosuio ()->output (1);
  exit (0);
}

void
udbctl_addgroup (int argc, char *argv[])
{
  if (optind >= argc)
    usage ();
  char *group (NULL);
  bool ok (true);
  for (int i = optind; i < argc; i++) {
    group = argv[i];
    bool thisok = create_group (group);
    aout << group << (thisok ? " " : " not ") << "ok\n";
    ok = ok && thisok;
  }
  group_db->checkpoint ();
  exit (!ok); // shell is backwards 
}

void
udbctl_listgroups (int argc, char *argv[])
{
  bool verbose = false;
  int ch;
  while ((ch = getopt (argc, argv, "v")) != -1)
    switch (ch) {
    case 'v':
      verbose = true;
      break;
    default:
      usage ();
      break;
    }

  /* Duplicates the function of the group list by access db directly */
  ptr<dbEnumeration> it = group_db->enumerate ();
  ptr<dbPair> d = it->nextElement ();
  while (d != NULL) {
    str groupname (d->key->value, d->key->len);
    unsigned int groupcount = 0;
    ptr<dbrec> rec = group_db->lookup (d->key);
    xdrmem x (rec->value, rec->len, XDR_DECODE);
    group_entry *group = New group_entry;
    bzero (group, sizeof (*group));
    if (xdr_group_entry (x.xdrp (), group)) {
      groupcount = group->articles.size ();
      aout << groupname << "\t" << groupcount << "\n";
      if (verbose) {
	// Dump the complete list of articles for this group
	for (size_t i = 0; i < group->articles.size (); i++)
	  aout << i << " " 
	       << group->articles[i].artno << " "
	       << group->articles[i].msgid << " "
	       << group->articles[i].blkid << "\n";
      }
      aout->flush ();
      xdr_delete (reinterpret_cast<xdrproc_t> (xdr_group_entry), group);
    } 
    d = it->nextElement();
  }
}

void
udbctl_listheaders (int argc, char *argv[])
{
  ptr<dbEnumeration> it = header_db->enumerate ();
  ptr<dbPair> d = it->nextElement ();
  while (d != NULL) {
    str id (d->key->value, d->key->len);
    aout << id << "\n";
    aout->flush ();
    d = it->nextElement();
  }
}

int
main (int argc, char *argv[])
{
  char *dirbase (NULL);
  setprogname (argv[0]);
  putenv ("POSIXLY_CORRECT=1"); // Prevents Linux from reordering options

  int ch;
  while ((ch = getopt (argc, argv, "d:")) != -1)
    switch (ch) {
    case 'd':
      dirbase = optarg;
      break;
    default:
      usage ();
      break;
    }
  if (optind >= argc)
    usage ();

  if (dirbase && chdir (dirbase) < 0)
    fatal << "chdir(" << dirbase << "): " << strerror (errno) << "\n";

  // Prepare to dispatch on command name
  const modevec *mp;
  for (mp = modes; mp->name; mp++)
    if (!strcmp (argv[optind], mp->name))
      break;
  if (!mp->name)
    usage ();
  udbctl_mode = mp;

  // Skip over command name...
  optind++;

  // XXX should only open the database that's needed
  dbOptions opts;
  group_db = New refcounted<dbfe> ();
  if (int err = group_db->opendb ("groups", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
  header_db = New refcounted<dbfe> ();
  if (int err = header_db->opendb ("headers", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
  
  mp->fn (argc, argv);
  
  return 0;
}
