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


int
store_block(sfs_ID key, void *data, int datasize) 
{
  dhash_insertarg i_arg;
  i_arg.key = key;
  i_arg.data.setsize(datasize);
  i_arg.type = DHASH_STORE;
  memcpy(i_arg.data.base (), data, datasize);
  
  dhash_stat res;

  int err = cp2p ()->scall(DHASHPROC_INSERT, &i_arg, &res);
  if (err) return -err;
  if (res != DHASH_OK) return res;

  return DHASH_OK;
}


int
fetch_block(int i, sfs_ID key, int datasize) 
{
  dhash_res res;

  int err = cp2p ()->scall(DHASHPROC_LOOKUP, &key, &res);

#define VERIFY
#ifdef VERIFY
  int diff = memcmp(data[i], res.resok->res.base (), datasize);
  assert (!diff);
#endif

  if (err) return -err;
  else
    return DHASH_OK;
  
}

sfs_ID
random_ID () {
  return random_bigint(NBIT);
}

//size must be word sized
sfs_ID
make_block(void *data, int size) 
{
  
  long *rd = (long *)data;
  for (unsigned int i = 0; i < size/sizeof(long); i++) 
    rd[i] = random();

  return random_ID ();
}

void
prepare_test_data(int num, int datasize) 
{
  IDs = new sfs_ID[num];
  data = (void **)malloc(sizeof(void *)*num);
  for (int i = 0; i < num; i++) {
    data[i] = malloc(datasize);
    IDs[i] = make_block(data[i], datasize);
  }
}

int
store(int num, int size) {
  prepare_test_data (num, size);
  
  for (int i = 0; i < num; i++) {
    //    struct timeval end;
    // struct timeval start;
    //gettimeofday(&start, NULL);
    int err = store_block(IDs[i], data[i], size);
    assert(err == 0);
    //gettimeofday(&end, NULL);
    //float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
    //fprintf(outfile, "%f\n", elapsed);
  }

  return 0;
}

int
fetch(int num, int size) {
  
  for (int i = 0; i < num; i++) {
    struct timeval end;
    struct timeval start;
    gettimeofday(&start, NULL);

    int err = fetch_block(i, IDs[i],  size);
    assert(err == DHASH_OK);
    gettimeofday(&end, NULL);
    float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
    fprintf(outfile, "%f\n", elapsed);
  }

  return 0;
}

#if 0
int 
fetch(int num, int size) {
  ptr<aclnt> clnt = cp2p();
  for (int i = 0; i < num; i++) {
    int err = fetch_block(i, IDs[i], size);
    assert(err == 0);
  }

  return 0;
}
#endif

void
usage(char *progname) 
{
  printf("%s: control_socket num_trials data_size file\n", progname);
  exit(0);
}

int
main (int argc, char **argv)
{

  sfsconst_init ();

  if (argc < 5) {
    usage (argv[0]);
    exit (1);
  }

  control_socket = argv[1];
  int num = atoi(argv[2]);
  int datasize = atoi(argv[3]);

  char *output = argv[4];
  if (strcmp(output, "-") == 0)
    outfile = stdout;
  else
    outfile = fopen(output, "w");
  
  store(num, datasize);
  fetch(num, datasize);

}
