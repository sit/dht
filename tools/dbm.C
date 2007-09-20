/*
 *
 * Copyright (C) 2003--2007 Emil Sit (sit@mit.edu),
 * Copyright (C) 2001--2004 Frank Dabek (fdabek@lcs.mit.edu), 
 *            Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <async.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include <dhblock.h>
#include <dhblock_keyhash.h>
#include <sfscrypt.h>
#include <sys/time.h>

// {{{ Declarations
class harness_t {
public:
  enum test_mode_t { FETCH, STORE };

protected:
  str ctlsock;	        // How to connect to lsd/dhash
  dhash_ctype ctype;    // Type of objects to store
  unsigned int nobj;    // Number of objects to store
  unsigned int objsize; // Size of each object stored
  unsigned int lifetime;// How long before objects expire
  unsigned int window;  // Number of operations to run in parallel
  unsigned int seed;    // Random seed

  unsigned int niters;  // Mutable objects may go multiple rounds

  dhashclient *dhash;
  ptr<option_block> options;

private:
  FILE *outfile;
  FILE *bwfile;

  test_mode_t mode;

  int bps;
  timecb_t *measurecb;
  void measure_bw ();

  struct timeval start;
  struct timeval end;
  unsigned int nundone;
  unsigned int lastsent;
  unsigned int out;
  vec<bool> done;

  void tcp_connect_cb (int fd);
  void connected ();
  void go (unsigned int iter);
  void init_iter ();
  void storecb (int iter, unsigned int dx, u_int64_t start,
      dhash_stat status, ptr<insert_info> i);
  void fetchcb (int iter, unsigned int i, u_int64_t start,
      dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path);
  void complete ();

  void eofhandler ();

protected:
  vec<chordID> IDs;
  virtual void prepare_test_data () = 0;
  virtual void store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb) = 0;
  virtual void fetch_one (unsigned int iter, unsigned int i, cb_cret cb) = 0;
  virtual bool verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk) = 0;

public:
  static harness_t *alloc (str cs, str ctype);
  harness_t (str cs, dhash_ctype ct);
  virtual ~harness_t ();

  virtual bool parse_argv (const vec<str> &argv);

  virtual void run (test_mode_t mode);
}; 

struct chash_harness_t : public harness_t {
  chash_harness_t (str cs) : harness_t (cs, DHASH_CONTENTHASH) {}
  ~chash_harness_t () {}
  vec<str> data;
  void prepare_test_data ();
  void store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb);
  void fetch_one (unsigned int iter, unsigned int i, cb_cret cb);
  bool verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk);
};

struct noauth_harness_t : public harness_t {
  noauth_harness_t (str cs) : harness_t (cs, DHASH_NOAUTH) {}
  ~noauth_harness_t () {}
  vec<vec<str> > data;
  void prepare_test_data ();
  void store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb);
  void fetch_one (unsigned int iter, unsigned int i, cb_cret cb);
  bool verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk);
};

struct keyhash_harness_t : public harness_t {
  static str rawkey;
  keyhash_harness_t (str cs) :
    harness_t (cs, DHASH_KEYHASH), sk (NULL) {}
  ~keyhash_harness_t () {}
  vec<vec<keyhash_payload> > data;
  ptr<sfspriv> sk;
  sfs_pubkey2 pk;
  void prepare_test_data ();
  void store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb);
  void fetch_one (unsigned int iter, unsigned int i, cb_cret cb);
  bool verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk);
};
// }}}

// {{{ Utility functions
void
usage (int stat) 
{
  warnx << "Usage: " << progname << " dhashsock store|fetch <ctype>"
    << " <nobj> <objsize> [option=value ...]\n";
  warnx << "Valid options include:\n"
    "\tlifetime=<count>[s|m|h|d|w|M|y]\n"
    "\tniters=<iterations>\n"
    "\tnops=<rpc window size>\n"
    "\tseed=<randseed>\n"
    "\tout=<outfile>\n"
    "\tbw=<enablebw+logfile>\n";
  if (stat < 0)
    return;
  exit (stat);
}

u_int64_t
getusec ()
{
  timeval tv;
  gettimeofday (&tv, NULL);
  return tv.tv_sec * INT64(1000000) + tv.tv_usec;
}

chordID
make_data (mstr &d)
{
  // size must be word sized
  //  assert (datasize % sizeof (long) == 0);
  
  char *rd = d.cstr ();
  for (unsigned int i = 0; i < d.len (); i++) 
    rd[i] = random ();
  rd[d.len () - 1] = 0;

  return compute_hash (rd, d.len ());
}

void
cleanup (harness_t *harness)
{
  if (harness)
    delete harness;
  exit (0);
}
// }}}

// {{{ harness_t
// {{{ Object initialization and cleanup
harness_t *
harness_t::alloc (str cs, str ctype)
{
  if (ctype == "chash" || ctype == "contenthash") {
    return New chash_harness_t (cs);
  }
  else if (ctype == "noauth") {
    return New noauth_harness_t (cs);
  }
  else if (ctype == "keyhash") {
    return New keyhash_harness_t (cs);
  }
  return NULL;
}

harness_t::harness_t (str cs, dhash_ctype ct) :
  ctlsock (cs),
  ctype (ct),
  nobj (32),
  objsize (8192),
  lifetime (0),
  window (1),
  seed (0),
  niters (1),
  dhash (NULL),
  options (NULL),
  outfile (stdout),
  bwfile (stdout),
  mode (STORE),
  bps (0),
  measurecb (NULL),
  nundone (nobj),
  lastsent (0),
  out (0)
{
}

harness_t::~harness_t ()
{
  if (dhash) {
    delete dhash;
    dhash = NULL;
  }
  for (size_t i = 0; i < done.size (); i++) {
    if (!done[i]) 
      warn << (IDs[i]>>144) << " not done\n";
  }
  if (outfile) {
    fclose (outfile);
  }
  if (bwfile) {
    fclose (bwfile);
  }
}

void
harness_t::eofhandler () 
{
  warn << "Unexpected EOF on transport: block too large?\n";
  complete ();
}

bool
harness_t::parse_argv (const vec<str> &argv)
{
  if (argv.size () < 2)
    usage (1);
  nobj = atoi (argv[0]);
  objsize = atoi (argv[1]);

  if (argv.size () > 2) {
    for (size_t i = 2; i < argv.size (); i++) {
      char *eoff = strchr (argv[i].cstr (), '=');
      if (!eoff)
	usage (1);
      str name = substr (argv[i], 0, eoff - argv[i].cstr ());
      str val  = str (eoff + 1);
      if (name == "nops") {
	window = atoi (val.cstr ());
      }
      else if (name == "seed") {
	seed = strtoul (val.cstr (), NULL, 10);
      }
      else if (name == "niters") {
	niters = atoi (val.cstr ());
      }
      else if (name == "out") {
	if (val != "-")
	  outfile = fopen (val, "w");
      }
      else if (name == "bw") {
	if (val != "-")
	  bwfile = fopen (val, "w");
	measurecb = delaycb (1, wrap (this, &harness_t::measure_bw));
      }
      else if (name == "lifetime") {
	u_int32_t factor = 1; // Default of seconds.
	char f = val[val.len () - 1];
	if (!isdigit (f))
	  val = substr (val, 0, val.len () - 1);
	char *err = NULL;
	lifetime = strtoul (val.cstr (), &err, 10);
	if (*err != '\0') {
	  warnx << "Bad lifetime " << val << "\n";
	  usage (1);
	}
	switch (f) {
	  case 'y': factor = 365 * 86400 + 6 * 3600; break;
	  case 'M': factor = 30 * 86400;     break;
	  case 'w': factor = 7 * 86400;      break;
	  case 'd': factor = 86400;          break;
	  case 'h': factor = 3600;	     break;
	  case 'm': factor = 60;	     break;
	  case 's': factor = 1;              break;
	  default:
	    if (!isdigit (f)) {
	      warnx ("Bad life factor '%c'\n", f); 
	      usage (1);
	    }
	}
	lifetime *= factor;
	options = New refcounted<option_block> ();
	options->flags = DHASHCLIENT_EXPIRATION_SUPPLIED;
	options->expiration = time (NULL) + lifetime;
      }
      else {
	warnx << "Unrecognized option " << name << "\n";
	usage (1);
      }
    }
  }

  srandom (seed);
  nundone = nobj;
  return true;
}
// }}}
// {{{ Bandwidth measurement
void
harness_t::measure_bw (void)
{
  float bw = objsize * bps; // we get called every second
  bw /= 1024; // convert to K.
  unsigned long long usecs = (getusec ()/1000);
  fprintf (bwfile, "%llu\t%6.2f KB/s\n", usecs, bw);
  bps = 0;
  measurecb = delaycb (1, wrap (this, &harness_t::measure_bw));
}
// }}}
// {{{ Initial connection
void
harness_t::run (test_mode_t m)
{
  mode = m;
  const char *cstr = ctlsock.cstr ();
  if (strchr (cstr, ':')) {
    char *port = strchr (cstr, ':');
    str host = substr (ctlsock, 0, port - cstr);
    port++; // point at port
    short i_port = atoi (port);
    warn << "Connecting to " << host << ":" << port << " via TCP...";
    tcpconnect (host, i_port,
	wrap (this, &harness_t::tcp_connect_cb));
  } else {
    dhash = New dhashclient (ctlsock);
    delaycb (0, wrap (this, &harness_t::connected));
  }
}

void
harness_t::tcp_connect_cb (int fd)
{
  if (fd < 0) 
    fatal << "connect failed\n";
  warnx << "... connected!\n";
  ptr<axprt_stream> xprt = axprt_stream::alloc (fd, 1024*1025);
  dhash = New dhashclient (xprt);
  delaycb (0, wrap (this, &harness_t::connected));
}
// }}}
// {{{ Main control
void
harness_t::init_iter ()
{
  if (done.size () != nobj)
    done.setsize (nobj);
  nundone = nobj;
  lastsent = 0;
  for (size_t i = 0; i < nobj; i++)
    done[i] = false;
}

void
harness_t::connected () 
{
  assert (niters > 0);
  assert (dhash);
  dhash->seteofcb (wrap (this, &harness_t::eofhandler));

  prepare_test_data ();
  init_iter ();
  gettimeofday (&start, NULL);
  go (0);
}

void
harness_t::go (unsigned int iter)
{
  if ((lastsent >= nobj) && (out == 0) && (nundone == 0)) {
    iter++;
    if (iter >= niters) {
      complete ();
      return;
    }
    init_iter ();
    warnx << "Starting round " << iter << "\n";
  }
  while (out < window && lastsent < nobj) {
    out++;
    if (mode == STORE)
      store_one (iter, lastsent,
	  wrap (this, &harness_t::storecb, iter, lastsent, getusec ()));
    else
      fetch_one (iter, lastsent,
	  wrap (this, &harness_t::fetchcb, iter, lastsent, getusec ()));
    lastsent++;
  }
}

void
harness_t::storecb (int iter, unsigned int dx, u_int64_t start,
    dhash_stat status, ptr<insert_info> i)
{
  out--;
  nundone--;
  done[dx] = true;

  strbuf s;
  if (status != DHASH_OK) {
    s << "store_cb: " << i->key << " " << status << "\n";
  } else {
    bps++;
    s << (i->key>>144) << " / " << (getusec () - start)/1000 << " /";
    for (size_t j = 0; j < i->path.size (); j++)
      s << " " << (i->path[j]>>144);
    s << "\n";
  }
  str buf (s);
  fprintf (outfile, "%s", buf.cstr ());
  if (outfile != stdout)
    warnx << buf;

  delaycb (0, wrap (this, &harness_t::go, iter));
}

void
harness_t::fetchcb (int iter, unsigned int i, u_int64_t start,
    dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path)
{
  out--;
  nundone--;
  done[i] = true;

  if (stat || !blk) {
    strbuf buf;
    buf << "Error: " << IDs[i] << " " << stat << "\n";
    fprintf (outfile, str (buf).cstr ());
  }
  else if (!verify_one (iter, i, blk))
    warnx << "verification failed for block " << IDs[i] << "\n";
  else {
    bps++;
    strbuf buf;
    buf << (IDs[i]>>144) << " " << (getusec () - start)/1000 << " /";
    for (u_int i = 0; i < blk->times.size (); i++)
      buf << " " << blk->times[i];
    buf << " /";

    buf << " " << blk->hops << " " <<  blk->errors
	<< " " << blk->retries;
    for (u_int i=0; i < path.size (); i++) {
      buf << " " << (path[i]>>144);
    }
    buf << " / " << blk->expiration;
    
    buf << "\n";
    fprintf (outfile, str (buf).cstr ());
    if (outfile != stdout)
      warnx << buf;
  } 

  delaycb (0, wrap (this, &harness_t::go, iter));
}

void
harness_t::complete ()
{
  gettimeofday (&end, NULL);
  float elapsed = (end.tv_sec - start.tv_sec)*1000.0 +
    (end.tv_usec - start.tv_usec)/1000.0;
  fprintf (outfile, "Total Elapsed: %f\n", elapsed);
  
  if (bwfile && measurecb) {
    timecb_remove (measurecb);
    measurecb = NULL;
    measure_bw ();
  }
  delete this;
  exit (0);
}
// }}}
// }}}
// {{{ Content type harnesses
// {{{ content hash
void
chash_harness_t::prepare_test_data (void)
{
  IDs.setsize (nobj);
  for (size_t i = 0; i < nobj; i++) {
    mstr x (objsize);
    IDs[i] = make_data (x);
    data.push_back (str (x));
  }
}

void
chash_harness_t::store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb)
{
  dhash->insert (data[i].cstr (), data[i].len (), cb, options);
}

void
chash_harness_t::fetch_one (unsigned int iter, unsigned int i, cb_cret cb)
{
  dhash->retrieve (IDs[i], DHASH_CONTENTHASH, cb);
}

bool
chash_harness_t::verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk)
{
  return (objsize == blk->data.len () && (data[i] == blk->data));
}
// }}}
// {{{ noauth
void
noauth_harness_t::prepare_test_data (void)
{
  IDs.setsize (nobj);
  // Generate all writes first.
  vec<str> obj;
  for (size_t i = 0; i < nobj * niters; i++) {
    mstr x (objsize);
    chordID id = make_data (x);
    if (i < nobj)
      IDs[i] = id;
    obj.push_back (str (x));
  }
  // Assign writes to objects by version, even though stored by key.
  for (size_t i = 0; i < nobj; i++) {
    vec<str> thisobj;
    for (size_t j = 0; j < niters; j++) {
      thisobj.push_back (obj[j*nobj + i]);
    }
    data.push_back (thisobj);
  }
}

void
noauth_harness_t::store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb)
{
  dhash->insert (IDs[i], data[i][iter].cstr (), data[i][iter].len (),
      cb, options, DHASH_NOAUTH);
}

void
noauth_harness_t::fetch_one (unsigned int iter, unsigned int i, cb_cret cb)
{
  dhash->retrieve (IDs[i], DHASH_NOAUTH, cb);
}

bool
noauth_harness_t::verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk)
{
  unsigned int nfound = 0;
  // Ensure that everything we've tried to store is available.
  // There may be more (e.g. vData.size () > (iter+1)).
  for (size_t odx = 0; odx < iter; odx++) {
    // The order of sub-objects in a noauth aren't guaranteed; search.
    for (size_t rdx = 0; rdx < blk->vData.size (); rdx++) {
      if (data[i][odx] == blk->vData[rdx]) {
	nfound++;
	break;
      }
    }
  }
  return nfound == iter;
}
// }}}
// {{{ keyhash
// The result of "sfskey gen -KP"
str keyhash_harness_t::rawkey = "SK1::AAAARAAAAACTeWZ+ZYJg7ZxNB6njxaiR2zsSAEL/0SgRCLumz+MX1SdIHt5HgZE57bByTWm67UNO/pq19X5Z/EXffF6xrOU/AAAARAAAAAHgRkhTAJ6dW/sNYo0/gkq8DUbhevwnnnDRfQnm3b+wm/TPLuZXipV8b0zoygUjculibuS014WydUbkzXoGFRbDF/5X4/H7qInquzII342aOasirycAAAAA:0x114ac1d08fa6f5e7f4d36c382d1ba8f2cd5321d92c9b0053f2e2f47663e5ea35bc75017da57a722338b8f3a7de776aefa036459c6bad1dc53e0b9235e1479848ee99d4d9f8ba1a3aa7e161ad9d13a578593706f16b6da5eafc2924aaae35187bbae6f8a0731ed4e87228b24cc864a23a94ed050de807963485fef32fa7a9108fd:x";

void
keyhash_harness_t::prepare_test_data (void)
{
  IDs.setsize (nobj);
  sk = sfscrypt.alloc_priv (rawkey, SFS_SIGN);
  assert (sk);
  sk->export_pubkey (&pk);
  // Generate all writes first.
  vec<str> obj;
  for (size_t i = 0; i < nobj * niters; i++) {
    mstr x (objsize);
    make_data (x);
    obj.push_back (str (x));
  }
  // Assign writes so that same seed with different niters will
  // share the same first min(niter1, niter2) writes.
  for (size_t i = 0; i < nobj; i++) {
    vec<keyhash_payload> thisobj;
    salt_t salt;
    bzero (salt, sizeof (salt));
    salt[0] =  i      & 0xFF;
    salt[1] = (i>>8)  & 0xFF;
    salt[2] = (i>>16) & 0xFF;
    salt[3] = (i>>24) & 0xFF;
    salt[4] =  seed      & 0xFF;
    salt[5] = (seed>>8)  & 0xFF;
    salt[6] = (seed>>16) & 0xFF;
    salt[7] = (seed>>24) & 0xFF;
    for (size_t j = 0; j < niters; j++) {
      keyhash_payload p (salt, j, obj[j*nobj + i]);
      thisobj.push_back (p);
    }
    IDs[i] = thisobj.back ().id (pk);
    data.push_back (thisobj);
  }
}

void
keyhash_harness_t::store_one (unsigned int iter, unsigned int i, cbinsertgw_t cb)
{
  sfs_sig2 s;
  sfs_pubkey2 pk;
  data[i][iter].sign (sk, pk, s);
  dhash->insert (IDs[i], pk, s, data[i][iter], cb, options);
}

void
keyhash_harness_t::fetch_one (unsigned int iter, unsigned int i, cb_cret cb)
{
  dhash->retrieve (IDs[i], DHASH_KEYHASH, cb);
}

bool
keyhash_harness_t::verify_one (unsigned int iter, unsigned int i, ptr<dhash_block> blk)
{
  ptr<keyhash_payload> p = keyhash_payload::decode (blk);
  return ((p->version () == data[i][iter].version ()) &&
      (strncmp (p->salt (), data[i][iter].salt (), sizeof (salt_t)) == 0) &&
      (p->buf () == data[i][iter].buf ()));
}
// }}}
// }}}

int
main (int argc, char **argv)
{
  harness_t *harness = NULL;
  setprogname (argv[0]);

  if (argc < 4) {
    usage (1);
  }
  vec<str> sargv;
  for (int i = 4; i < argc; i++)
    sargv.push_back (str (argv[i]));

  harness = harness_t::alloc (argv[1], argv[3]);
  if (!harness) {
    warn << "Invalid ctype " << argv[3] << "\n";
    usage (1);
  }

  sigcb (SIGTERM, wrap (&cleanup, harness));
  sigcb (SIGINT, wrap (&cleanup, harness));
  sigcb (SIGHUP, wrap (&cleanup, harness));

  if (!harness->parse_argv (sargv)) {
    usage (1);
  }
  if (!strcasecmp (argv[2], "store")) {
    harness->run (harness_t::STORE);
  } else if (!strcasecmp (argv[2], "fetch")) {
    harness->run (harness_t::FETCH);
  }

  amain ();
}

// vim: foldmethod=marker
