#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include <sfscrypt.h>
#include <sys/time.h>
#include "modlogger.h"

//  ip:11978

#define NEWBLOCK 1
#define WRITE 2
#define READ 3
#define DATASIZE 8192
#define trace modlogger ("pkc")

ptr<sfspriv> sk;
sfs_pubkey2 pk;
sfs_sig2 s;
char data[DATASIZE];
int interval;
char out[100];

void pk_read (dhashclient, chordID);
void pk_write_cb (dhashclient, long, dhash_stat, ptr<insert_info>);

void 
insert_cb (dhash_stat stat, ptr<insert_info> i)
{
  if (stat != DHASH_OK) {
    warn << "Insert error dhash_stat: " << stat << "\n";
    exit (-1);
  } else {
    warn << "Insert successful\n";
    exit (1);
  }    
}

void 
pk_read_cb (dhashclient dhash, dhash_stat stat, ptr<dhash_block> blk, 
	    vec<chordID> path)
{
  if (stat == DHASH_OK) {
    timeval tp;
    gettimeofday (&tp, NULL);
    
    ptr<keyhash_payload> p = keyhash_payload::decode (blk);
    
    trace << tp.tv_sec << "." << tp.tv_usec << ": "
	  << "READ key " << p->id (pk)
	  << " version " << p->version ()  << "\n";

    delaycb (interval, wrap (&pk_read, dhash, p->id (pk)));

  } else {
    warn << "READ error dhash_stat: " << stat << "\n";
    exit (-1);
  }
}

void 
pk_read (dhashclient dhash, chordID key)
{
  dhash.retrieve (key, DHASH_KEYHASH, wrap (&pk_read_cb, dhash));
}

void 
pk_write (dhashclient dhash, long version)
{
  keyhash_payload newp (version + 1, str (data, DATASIZE));
  sfs_pubkey2 pk;
  sfs_sig2 s;
  newp.sign (sk, pk, s);
  
  dhash.insert (newp.id (pk), pk, s, newp, 
		wrap (&pk_write_cb, dhash, newp.version ()));
}

void 
pk_write_cb (dhashclient dhash, long version, 
	     dhash_stat stat, ptr<insert_info> i)
{
  if (stat == DHASH_OK) {
    timeval tp;
    gettimeofday (&tp, NULL);
    
    trace << tp.tv_sec << "." << tp.tv_usec << ": "
	  << "WROTE key " << i->key 
	  << "  version " << version << "\n";

    delaycb (interval, wrap (&pk_write, dhash, version));
  } else {
    warn << "WRITE error dhash_stat: " << stat << "\n";
    exit (-1);
  }
}

void 
pk_write_init_cb (dhashclient dhash, dhash_stat stat, 
		  ptr<dhash_block> blk, vec<chordID> path)
{
  ptr<keyhash_payload> p = keyhash_payload::decode (blk);
  keyhash_payload newp (p->version () + 1, str (data, DATASIZE));
  sfs_pubkey2 pk;
  sfs_sig2 s;
  newp.sign (sk, pk, s);
 
  dhash.insert (newp.id (pk), pk, s, newp, wrap (&pk_write_cb, dhash, 
						 newp.version ()));
}

static void
usage ()
{
  warnx << "usage: " << progname
	<< " sock mode frequency[ops/minute] pubkeyfile \n";
  exit (1);
}

str control_socket;

int 
main (int argc, char **argv)
{
  setprogname (argv[0]);
  
  if (argc < 4)
    usage ();

  control_socket = argv[1];
  dhashclient dhash (control_socket);

  int freq = atoi (argv[3]); //writes and reads per minute
  interval = 60/freq; // in seconds
  str key = file2wstr (argv[4]);
  sk = sfscrypt.alloc_priv (key, SFS_SIGN);

  keyhash_payload p (0, str (data, DATASIZE));
  p.sign (sk, pk, s);

  int logfd = open ("pkc.log", O_WRONLY | O_APPEND | O_CREAT, 0666);
  modlogger::setlogfd (logfd);
  
  switch (atoi (argv[2])) {
  case NEWBLOCK: {
    ptr<option_block> opt = New refcounted<option_block>;
    opt->flags = DHASHCLIENT_NEWBLOCK;
    dhash.insert (p.id (pk), pk, s, p, wrap (&insert_cb), opt);    
    break;
  }
  case WRITE: {
    dhash.retrieve (p.id (pk), DHASH_KEYHASH, wrap (&pk_write_init_cb, dhash));
    break;
  }
  case READ: {
    //warn << "Reading bID " << p.id (pk) << "\n"; 
    dhash.retrieve (p.id (pk), DHASH_KEYHASH, wrap (&pk_read_cb, dhash));
    break;
  }
  default:
    usage ();
  }

  amain ();
}











