#include "sfsmisc.h"
#include "dhash_prot.h"
#include "dhash.h"
#include "crypt.h"
#include <sys/time.h>

float avg_lookupRPCs;

static ptr<aclnt> p2pclnt;
static bigint *IDs;
static void **data;
static str control_socket;
static FILE *outfile;

int out = 0;
int out_op = 0;
int OPS_OUT = 1024;

void afetch_cb (dhash_res *res, chordID key, char *buf, int i, struct timeval start, clnt_stat err);
void afetch_cb2 (dhash_res *res, char *buf, unsigned int *read, int i, struct timeval start, clnt_stat err);

ref<aclnt>
cp2p ()
{
  int fd;

  if (p2pclnt)
    return p2pclnt;
  fd = unixsocket_connect (control_socket);
  if (fd < 0)
      fatal ("%s: %m\n", control_socket.cstr ());
  p2pclnt = aclnt::alloc (axprt_unix::alloc (fd), dhashclnt_program_1);
  return p2pclnt;
}

#define MTU 8192

int
store_block(chordID key, void *data, unsigned int datasize) 
{


  dhash_storeres res;    
  dhash_insertarg *i_arg = New dhash_insertarg ();
  i_arg->key = key;  
  int n = (MTU < datasize) ? MTU : datasize;
  i_arg->data.setsize(n);
  i_arg->type = DHASH_STORE;
  i_arg->attr.size = datasize;
  i_arg->offset = 0;
  memcpy(i_arg->data.base (), (char *)data, n);
  clnt_stat err = cp2p ()->scall(DHASHPROC_INSERT, i_arg, &res);
  if (err) warn << "RPC error: " << err << "\n";
  if (err) return -err;

  unsigned int written = n;
  delete i_arg;
  chordID source = res.resok->source;

  while (!res.resok->done) {
    n = (written + MTU < datasize) ? MTU : datasize - written;
    dhash_send_arg *sarg = New dhash_send_arg();
    sarg->dest = source;
    sarg->iarg.key = key;
    sarg->iarg.data.setsize(n);
    memcpy (sarg->iarg.data.base (), (char *)data + written, n);
    sarg->iarg.type = DHASH_STORE;
    sarg->iarg.attr.size = datasize;
    sarg->iarg.offset = written;
    
    clnt_stat err = cp2p ()->scall(DHASHPROC_SEND, sarg, &res);
    if (err) warn << "RPC error: " << err << "\n";
    if (err) return -err;
    if (res.status != DHASH_OK) return res.status;
    written += n;
    delete sarg;
  };

  return DHASH_OK;
}


int
fetch_block(int i, chordID key, int datasize) 
{
  dhash_res res;
  char *buf = New char[datasize];

  dhash_fetch_arg arg;
  arg.key = key;
  arg.len = MTU;
  arg.start = 0;
  int err = cp2p ()->scall(DHASHPROC_LOOKUP, &arg, &res);
  assert (err == 0);
  if (res.status != DHASH_OK) 
    warn << "error " << res.status << "fetching data\n";

  memcpy(buf, res.resok->res.base (), res.resok->res.size ());
  unsigned int read = res.resok->res.size ();
  
  while (read < res.resok->attr.size) {
    arg.len = (read + MTU < res.resok->attr.size) ? MTU : 
      res.resok->attr.size - read;
    arg.start = read;
    err = cp2p ()->scall(DHASHPROC_LOOKUP, &arg, &res);
    if (res.status != DHASH_OK) 
      warn << "error fetching data\n";
    memcpy(buf + read, res.resok->res.base (), res.resok->res.size ());
    read += res.resok->res.size ();
  };
  
#define VERIFY
#ifdef VERIFY
  int diff = memcmp(data[i], buf, datasize);
  assert (!diff);
#endif

  delete buf;

  if (err) return -err;
  else
    return DHASH_OK;
  
}

int
fetch_block_async(int i, chordID key, int datasize) 
{
  dhash_res *res = New dhash_res (DHASH_OK);
  char *buf = New char[datasize];
  struct timeval start;
  gettimeofday (&start, NULL);

  dhash_fetch_arg arg;
  arg.key = key;
  arg.len = MTU;
  arg.start = 0;
  out++;
  cp2p ()->call(DHASHPROC_LOOKUP, &arg, res, wrap(&afetch_cb, res, key, buf, i, start));
  return 0;
}

void
finish (char *buf, unsigned int *read, 
	struct timeval start, 
	int i,
	dhash_res *res) 
{
  out--;
      

#ifdef VERIFY
    int diff = memcmp(data[i], buf, *read);
    assert (!diff);
#endif

    struct timeval end;
    gettimeofday(&end, NULL);
    float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
    fprintf(outfile, "%f %d\n", elapsed, res->resok->hops);
    
    delete read;
    delete buf;
}

void
afetch_cb (dhash_res *res, chordID key, char *buf, int i, struct timeval start, clnt_stat err) 
{
  if (err) {
    strbuf sb;
    rpc_print (sb, key, 5, NULL, " ");
    str s = sb;
    fprintf(outfile, "RPC error: %d %s", err, s.cstr());
    out--;
    return;
  }

  if (res->status != DHASH_OK) {
    strbuf sb;
    rpc_print (sb, key, 5, NULL, " ");
    str s = sb;
    fprintf(outfile, "Error: %d %s", res->status, s.cstr());
    out--;
    return;
  }

  memcpy(buf, res->resok->res.base (), res->resok->res.size ());
  unsigned int *read = New unsigned int(res->resok->res.size ());
  unsigned int off = res->resok->res.size ();

  if (off == res->resok->attr.size) finish (buf, read, start, i, res);
  while (off < res->resok->attr.size) {
    ptr<dhash_transfer_arg> arg = New refcounted<dhash_transfer_arg> ();

    arg->farg.key = key;
    arg->farg.len = (off + MTU < res->resok->attr.size) ? MTU : 
      res->resok->attr.size - off;
    arg->farg.start = off;
    arg->source = res->resok->source;
    dhash_res *nres = New dhash_res(DHASH_OK);
    out_op++;
    cp2p ()->call(DHASHPROC_TRANSFER, arg, nres, 
		  wrap(&afetch_cb2, nres, buf, read, i, start));
    off += arg->farg.len;
  };
  delete res;
}

void
afetch_cb2 (dhash_res *res, char *buf, unsigned int *read, int i, struct timeval start, clnt_stat err) 
{
  if (err) {
    out_op--;
    warn << "err: " << err << "\n";
    return;
  }
  assert(err == 0);
  assert(res->status == DHASH_OK);
  memcpy(buf + res->resok->offset, res->resok->res.base (), res->resok->res.size ());
  *read += res->resok->res.size ();
  out_op--;
  if (*read == res->resok->attr.size) finish (buf, read, start, i, res);
  
  delete res;
}

chordID
random_ID () {
  return random_bigint(NBIT);
}

//size must be word sized
chordID
make_block(void *data, int size) 
{
  
  long *rd = (long *)data;
  for (unsigned int i = 0; i < size/sizeof(long); i++) 
    rd[i] = random();

  char id[sha1::hashsize];
  sha1_hash (id, rd, size);
  chordID ID;
  mpz_set_rawmag_be (&ID, id, sizeof (id));  // For big endian
  return ID;
}

void
prepare_test_data(int num, int datasize) 
{
  IDs = New chordID[num];
  data = (void **)malloc(sizeof(void *)*num);
  for (int i = 0; i < num; i++) {
    data[i] = malloc(datasize);
    IDs[i] = make_block(data[i], datasize);
  }
}

int
store(int num, int size) {
  
  for (int i = 0; i < num; i++) {
    int err = store_block(IDs[i], data[i], size);
    if (err) warn << "ERROR: " << err << "\n";
  }

  return 0;
}

int
fetch(int num, int size) {

  for (int i = 0; i < num; i++) {
    fetch_block_async(i, IDs[i],  size);
    while (out > OPS_OUT) acheck ();
  }

  while (out > 0) acheck();
  return 0;
}

void
usage(char *progname) 
{
  printf("%s: vnode_num control_socket num_trials data_size file <f or s> nops\n", 
	 progname);
  exit(0);
}

int
main (int argc, char **argv)
{


  dhash_fetchiter_res *res = New dhash_fetchiter_res (DHASH_CONTINUE);
  res = NULL;

  sfsconst_init ();

  if (argc < 8) {
    usage (argv[0]);
    exit (1);
  }

  control_socket = argv[2];
  int num = atoi(argv[3]);
  int datasize = atoi(argv[4]);

  char *output = argv[5];
  if (strcmp(output, "-") == 0)
    outfile = stdout;
  else
    outfile = fopen(output, "w");

  if (!outfile) {
    printf ("could not open %s\n", output);
    exit(1);
  }

  int i = atoi(argv[1]);
  dhash_stat ares;
  cp2p ()->scall(DHASHPROC_ACTIVE, &i, &ares);

  prepare_test_data (num, datasize);

  OPS_OUT = atoi(argv[7]);

  struct timeval start;
  gettimeofday (&start, NULL);

  if (argv[6][0] == 's')
    store(num, datasize);
  else
    fetch(num, datasize);
  
  struct timeval end;
  gettimeofday (&end, NULL);
  float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
  fprintf(outfile, "Total Elapsed: %f\n", elapsed);
}
