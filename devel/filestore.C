#define BLOCKSIZE 16384
// supports files up to (BLOCKSIZE-260)/20 * BLOCKSIZE/20 * BLOCKSIZE
// currently:  10815307776

#include <chord.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashclient.h>

dhashclient *dhash;
int inflight = 0;
FILE *outfile;

// indirect block
struct indirect {
  vec<chordID> hs;

  void add_hash(chordID h) {
    hs.push_back(h);
  }

  int len(void) {
    return hs.size() * sha1::hashsize;
  }

  bool full(void) {
    return (len() + sha1::hashsize) > BLOCKSIZE;
  }

  void print(char *buf, int l) {
    if(len() > l) {
      warnx << len() << "\n";
      fatal("buf too small\n");
    }

    for(unsigned int i=0; i<hs.size(); i++) {
      mpz_get_raw (buf, sha1::hashsize, &hs[i]);
      buf += sha1::hashsize;
    }
  }

  void clear(void) {
    hs.clear();
  }
};

// inode block
struct inode : indirect {
  char filename[256];
  int filelen;
  static const int extralen = sizeof(inode::filename) + sizeof(inode::filelen);

  inode(char *aname, int alen) {
    strncpy(filename, aname, sizeof(filename));
    filelen = alen;
  }

  int len(void) {
    return indirect::len() + extralen;
  }

  void print(char *buf, int l) {
    if(len() > l) {
      warnx << len() << "\n";
      fatal("buf too small\n");
    }

    memcpy(buf, filename, sizeof(filename));
    buf += sizeof(filename);
    memcpy(buf, &filelen, sizeof(filelen));
    buf += sizeof(filelen);

    indirect::print(buf, l - extralen);
  }
};

void insert_cb(dhash_stat s, ptr<insert_info>) {
  if(s != DHASH_OK)
    warn << "bad store\n";

  inflight--;
  if(inflight == 0)
    exit(0);
}

chordID write_block(char *buf, int len) {
  char hashbytes[sha1::hashsize];
  chordID ID;

  sha1_hash (hashbytes, buf, len);
  mpz_set_rawmag_be (&ID, hashbytes, sizeof (hashbytes));  // For big endian
  dhash->insert (ID, buf, len, wrap(&insert_cb));
  inflight++;

  return ID;
}

void list_cb(dhash_stat st, ptr<dhash_block> bl, vec<chordID> vc) {
  if(st != DHASH_OK)
    fatal("lost inode\n");

  str fns(bl->data, 256);
  char filename[256];
  strcpy(filename, fns);
  int filelen;
  memcpy(&filelen, bl->data+256, sizeof(filelen));

  warnx << filename << ": " << filelen << " bytes\n";
  exit(0);
}

void gotblock_cb(int len, dhash_stat st, ptr<dhash_block> bl,
		 vec<chordID> vc) {
  if(st != DHASH_OK) {
    warnx << "at " << len;
    fatal(" lost block\n");
  }

  if(fseek(outfile, len, SEEK_SET) != 0)
    fatal("fseek failure\n");
  if(fwrite(bl->data, 1, bl->len, outfile) != bl->len)
    fatal("write failure\n");

  inflight--;
  if(inflight == 0)
    exit(0);
}

void gotindirect_cb(int len, dhash_stat st, ptr<dhash_block> bl,
		    vec<chordID> vc) {
  if(st != DHASH_OK) {
    warnx << "at " << len;
    fatal(" lost indirect\n");
  }

  char *buf = bl->data;
  chordID ID;
  for(unsigned int i=0; i<bl->len; i+=20) {
    mpz_set_rawmag_be (&ID, buf+i, sha1::hashsize);  // For big endian
    dhash->retrieve(ID, wrap(&gotblock_cb, len+BLOCKSIZE*i/20));
    inflight++;
  }
}

void gotinode_cb(dhash_stat st, ptr<dhash_block> bl, vec<chordID> vc) {
  if(st != DHASH_OK)
    fatal("lost inode\n");

  str fns(bl->data, 256);
  char filename[256];
  strcpy(filename, fns);
  int filelen;
  memcpy(&filelen, bl->data+256, sizeof(filelen));

  outfile = fopen(filename, "w");
  if(outfile == NULL)
    fatal("can't open file for writing\n");

  char *buf = bl->data+inode::extralen;
  chordID ID;
  for(unsigned int i=0; i<(bl->len-inode::extralen); i+=20) {
    mpz_set_rawmag_be (&ID, buf+i, sha1::hashsize);  // For big endian
    dhash->retrieve(ID, wrap(&gotindirect_cb, (BLOCKSIZE/20)*BLOCKSIZE*i/20));
  }
}

int main(int argc, char *argv[]) {
  if(argc != 4)
    fatal("filestore [sockname] -[fls] [filename/hash]\n");

  dhash = New dhashclient(argv[1]);
  chordID ID;

  if(!strcmp(argv[2], "-s")) {
    // store
    struct stat st;
    if(stat(argv[3], &st) == -1)
      fatal("couldn't stat file\n");

    inode n(argv[3], st.st_size);

    FILE *f = fopen(argv[3], "r");
    if(f == NULL)
      fatal("couldn't open file\n");

    char buf[BLOCKSIZE];
    int len;
    indirect in;
    do {
      len = fread(buf, 1, BLOCKSIZE, f);
      ID = write_block(buf, len);

      in.add_hash(ID);
      if(in.full()) {
	// write out indirect block
	in.print(buf, BLOCKSIZE);
	ID = write_block(buf, in.len());
	in.clear();
	n.add_hash(ID);
      }
    } while(len == BLOCKSIZE);

    in.print(buf, BLOCKSIZE);
    ID = write_block(buf, in.len());
    n.add_hash(ID);
    n.print(buf, BLOCKSIZE);
    ID = write_block(buf, n.len());
    warnx << ID << "\n";
  } else if(!strcmp(argv[2], "-l")) {
    // list
    str2chordID(argv[3], ID);
    dhash->retrieve(ID, wrap(&list_cb));
  } else if(!strcmp(argv[2], "-f")) {
    // retrieve
    str2chordID(argv[3], ID);
    dhash->retrieve(ID, wrap(&gotinode_cb));
  } else {
    fatal("filestore [sockname] -[fls] [filename/hash]\n");
  }

  amain();
  return 0;
}
