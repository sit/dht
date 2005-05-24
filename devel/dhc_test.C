#include "dhash_common.h"
#include "dhashclient.h"
#include "dhblock_keyhash.h"
#include <sfscrypt.h>
#include <sys/time.h>

#define NEWBLOCK 1
#define WRITE 2
#define READ 3

#define DATASIZE 8192
char data[DATASIZE];
str control_socket;
u_int64_t start_insert, end_insert;
ptr<sfspriv> sk;

int insert_count, read_count, count;
u_int64_t start_massive_insert, end_massive_insert;
u_int64_t start_massive_read, end_massive_read;
u_int64_t total_massive_time;
char out[100];
int fd = 0;

void readonly_cb (chordID key, dhashclient dhash, dhash_stat stat, 
		  ptr<dhash_block> blk, vec<chordID> path);

keyhash_payload mp;
sfs_pubkey2 mpk;
sfs_sig2 ms;

void 
write_cb (dhashclient dhash, dhash_stat stat, ptr<insert_info> i)
{
  timeval tp;
  if (stat == DHASH_OK) {
    gettimeofday (&tp, NULL);
    end_massive_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    total_massive_time += (end_massive_insert - start_massive_insert);
    insert_count++;
  } else {
    warn << "write_cb err dhash_stat: " << stat << "\n";
    exit (-1);
  }

#if 1
  if (insert_count < count) {
    mp = keyhash_payload (insert_count+2, str (data, DATASIZE));
    mp.sign (sk, mpk, ms);
    gettimeofday (&tp, NULL);
    start_massive_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    dhash.insert (mp.id (mpk), mpk, ms, mp, wrap (&write_cb, dhash), NULL);
  }
#endif
  if (insert_count == count) {
#if 1
    warn << "DHC_TEST: Massive Insert Successful \n";
    warn << "           elapse time " << total_massive_time
	 << " usecs\n";
    sprintf (out, "%llu WRITE %d %llu usecs\n", end_massive_insert, count,
	     total_massive_time);
#else
    gettimeofday (&tp, NULL);
    end_massive_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    warn << "DHC_TEST: Massive Insert Successful \n";
    warn << "           elapse time " << end_massive_insert - start_massive_insert
	 << " usecs\n";
    sprintf (out, "%llu WRITE %d %llu usecs\n", end_massive_insert, count,
	     end_massive_insert - start_massive_insert);
#endif
    write (fd, out, strlen (out));
    exit (1);
  }
}

void 
newblock_cb (dhashclient dhash, dhash_stat stat, ptr<insert_info> i)
{
  timeval tp;
  gettimeofday (&tp, NULL);
  end_insert = tp.tv_sec * (u_int64_t)1000000 + tp.tv_usec;

  if (stat != DHASH_OK) {
    warn << "DHC NEWBLOCK error dhash_stat: " << stat << "\n";
    exit (-1);
  }
  else {
    warn << "DHC NEWBLOCK insert successful\n";
    warn << "DHC End Insert at " << end_insert << "\n";
    warn << "      elapse time " << end_insert - start_insert << " usecs\n";
    sprintf (out, "%llu INSERT_NEW 1 %llu usecs\n", end_insert, 
	     end_insert - start_insert);
    write (fd, out, strlen (out));
    exit (1);
  }
}

void
readonly_cb (chordID key, dhashclient dhash, dhash_stat stat, 
	     ptr<dhash_block> blk, vec<chordID> path)
{
  if (!blk) {
    warn << "DHC READ error dhash_stat: " << stat << "\n";
    exit (-1);
  } else
    if (++read_count >= count) {
      timeval tp;
      gettimeofday (&tp, NULL);
      end_massive_read = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;      
      warn << "DHC Massive READ successful\n";
      warn << "           elapsed time: " 
	   << end_massive_read - start_massive_read 
	   << " for " << read_count 
	   << " reads\n";
      sprintf (out, "%llu READ %d %llu usecs\n", end_massive_read, read_count,
	       end_massive_read - start_massive_read);
      write (fd, out, strlen (out));
      exit (1);
    } else 
      dhash.retrieve (key, DHASH_KEYHASH, wrap (&readonly_cb, key, dhash));
}

static void 
usage ()
{
  warnx << "usage: " << progname
	<< " sock randseed mode count[>10] pubkeyfile \n";
  exit (1);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  if (argc < 5)
    usage ();

  control_socket = argv[1];
  dhashclient dhash (control_socket);

  unsigned int seed = strtoul (argv[2], NULL, 10);
  srandom (seed);

  count = atoi (argv[4]);

  str key = file2wstr (argv[5]);
  sk = sfscrypt.alloc_priv (key, SFS_SIGN);

  total_massive_time = 0;

  timeval tp;
  gettimeofday (&tp, NULL);
  sprintf (out, "etna-test");
  fd = open (out, O_WRONLY | O_APPEND | O_CREAT, 0777);

  switch (atoi (argv[3])) {
  case NEWBLOCK: {
    keyhash_payload p (0, str (data, DATASIZE));
    sfs_pubkey2 pk;
    sfs_sig2 s;
    p.sign (sk, pk, s);

    ptr<option_block> opt = New refcounted<option_block>;
    opt->flags = DHASHCLIENT_NEWBLOCK;
    timeval tp;
    gettimeofday (&tp, NULL);
    start_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    warn << "DHC Start Insert NewBlock at " << start_insert << "\n";
    dhash.insert (p.id (pk), pk, s, p, wrap (&newblock_cb, dhash), opt);    
    break;
  }
  case WRITE: {
#if 1
    keyhash_payload p (1, str (data, DATASIZE));
    sfs_pubkey2 pk;
    sfs_sig2 s;
    p.sign (sk, pk, s);

    insert_count = 0;
    timeval tp;
    gettimeofday (&tp, NULL);
    start_massive_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    warn << "DHC Start Write Block at " << start_massive_insert << "\n";

    dhash.insert (p.id (pk), pk, s, p, wrap (&write_cb, dhash));    
#else
    insert_count = 0;
    timeval tp;
    gettimeofday (&tp, NULL);
    start_massive_insert = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    warn << "DHC Start Write Block at " << start_massive_insert << "\n";
    if (count <= 10) 
      for (int i=0; i<count; i++) {
	keyhash_payload p (i+1, str (data, DATASIZE));
	sfs_pubkey2 pk;
	sfs_sig2 s;
	p.sign (sk, pk, s);
	dhash.insert (p.id (pk), pk, s, p, wrap (&write_cb, dhash));    
      }
#endif    
    break;
  }    
  case READ: {
    keyhash_payload p (1, str (data, DATASIZE));
    sfs_pubkey2 pk;
    sfs_sig2 s;
    p.sign (sk, pk, s);

    read_count = 0;
    timeval tp;
    gettimeofday (&tp, NULL);
    start_massive_read = tp.tv_sec * (u_int64_t) 1000000 + tp.tv_usec;
    dhash.retrieve (p.id (pk), DHASH_KEYHASH, 
		    wrap (&readonly_cb, p.id (pk), dhash));
#if 0
    if (count < 10) 
      for (int i=0; i<count; i++)
	dhash.retrieve (p.id (pk), DHASH_KEYHASH, 
			wrap (&readonly_cb, p.id (pk), dhash));
    else
      for (int i=0; i<10; i++)
	dhash.retrieve (p.id (pk), DHASH_KEYHASH, 
			wrap (&readonly_cb, p.id (pk), dhash));
#endif
    break;
  }
  default:
    usage ();
  }

  amain ();
  close (fd);
}














