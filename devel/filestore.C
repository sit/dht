#define BLOCKSIZE 16384
// supports files up to (BLOCKSIZE-260)/20 * BLOCKSIZE/20 * BLOCKSIZE
// currently:  10815307776 bytes

#include <chord.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <dhash.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include <verify.h>

dhashclient *dhash;
int inflight = 0;
FILE *outfile;

// write block functions ----------------------------------

void insert_cb(dhash_stat s, ptr<insert_info>) {
  if(s != DHASH_OK)
    warn << "bad store\n";

  inflight--;
  if(inflight == 0)
    exit(0);
}

chordID write_block(char *buf, int len) {
  chordID ID = compute_hash(buf, len);
  dhash->insert (ID, buf, len, wrap(&insert_cb));
  inflight++;

  return ID;
}


// indirect block -----------------------------------

struct indirect {
  vec<chordID> hs;
  indirect *parent;

  void add_hash(chordID h) {
    hs.push_back(h);

    if(full()) {
      // write out indirect block
      write_out();
    }
  }

  virtual void print(char *buf, int l) {
    if(len() > l) {
      warnx << len() << "\n";
      fatal("buf too small\n");
    }

    for(unsigned int i=0; i<hs.size(); i++) {
      mpz_get_raw (buf, sha1::hashsize, &hs[i]);
      buf += sha1::hashsize;
    }
  }

  chordID write_out(void) {
    chordID ID;
    char buf[BLOCKSIZE];
    
    if(len() == 0)
      return 0; // xxx bug for max-sized file?

    print(buf, BLOCKSIZE);
    warnx << "len " << len() << "\n";
    ID = write_block(buf, len());
    clear();

    if(parent) {
      warnx << "p\n";
      parent->add_hash(ID);
    }

    return ID;
  }

  void clear(void) {
    hs.clear();
  }

  virtual int len(void) {
    return hs.size() * sha1::hashsize;
  }

  bool full(void) {
    return (len() + sha1::hashsize) > BLOCKSIZE;
  }

  indirect(indirect *p) : parent(p) {}
  virtual ~indirect() {}
};


// inode block -------------------------------------
// this is similar to an indirect block, but prepends
// a file name and length to it.

struct inode : indirect {
  char filename[256];
  int filelen;
  unsigned int extralen, blen;
  char *buf;

  inode(char *_name, int _len) : indirect(NULL) {
    extralen = sizeof(filename) + sizeof(filelen);
    strncpy(filename, _name, sizeof(filename));
    filelen = _len;
  }

  inode(ptr<dhash_block> blk) : indirect(NULL) {
    extralen = sizeof(filename) + sizeof(filelen);
    if(blk->len < extralen)
      fatal("incorrect format of inode block\n");

    strncpy(filename, blk->data, sizeof(filename));
    memcpy(&filelen, blk->data + sizeof(filename), sizeof(filelen));
    buf = blk->data + extralen;
    blen = blk->len - extralen;
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

  void write_out(void) {
    warnx << indirect::write_out() << "\n";
  }
};


// callback for printing out inode info --------------------------

void list_cb(dhash_stat st, ptr<dhash_block> bl, vec<chordID> vc) {
  if(st != DHASH_OK)
    fatal("lost inode\n");

  inode i(bl);
  warnx << i.filename << ": " << i.filelen << " bytes\n";

  exit(0);
}


// a chain of callbacks for retrieving indirect and file data blocks,
// to retrieve the file ----------------------------------------------

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
  for(unsigned int i=0; i<bl->len; i+=sha1::hashsize) {
    mpz_set_rawmag_be (&ID, buf+i, sha1::hashsize);  // For big endian
    dhash->retrieve(ID, wrap(&gotblock_cb, len+(i*BLOCKSIZE/sha1::hashsize)));
    warnx << "retrieve " << ID << "\n";
    inflight++;
  }
}

void gotinode_cb(dhash_stat st, ptr<dhash_block> bl, vec<chordID> vc) {
  if(st != DHASH_OK)
    fatal("lost inode\n");

  inode in(bl);

  outfile = fopen(in.filename, "w");
  if(outfile == NULL)
    fatal("can't open file for writing\n");

  chordID ID;
  for(unsigned int i=0; i<(in.blen); i+=sha1::hashsize) {
    mpz_set_rawmag_be(&ID, in.buf+i, sha1::hashsize);  // For big endian
    dhash->retrieve(ID, wrap(&gotindirect_cb, (BLOCKSIZE/sha1::hashsize)*(i*BLOCKSIZE/20)));
    warnx << "retrieve " << ID << "\n";
  }
}

void store(char *name) {
  struct stat st;
  if(stat(name, &st) == -1)
    fatal("couldn't stat file\n");

  inode n(name, st.st_size);

  FILE *f = fopen(name, "r");
  if(f == NULL)
    fatal("couldn't open file\n");

  char buf[BLOCKSIZE];
  int len;
  indirect in(&n);
  chordID ID;
  do {
    len = fread(buf, 1, BLOCKSIZE, f);
    warnx << "len " << len << "\n";
    ID = write_block(buf, len);

    in.add_hash(ID);
  } while(len == BLOCKSIZE);

  in.write_out();
  n.write_out();
}

int main(int argc, char *argv[]) {
  if(argc != 4)
    fatal("filestore [sockname] -[fls] [filename/hash]\n");

  dhash = New dhashclient(argv[1]);
  chordID ID;
  char *cmd = argv[2];
  char *name = argv[3];

  if(!strcmp(cmd, "-s")) {
    // store
    store(name);

  } else if(!strcmp(cmd, "-l")) {
    // list
    str2chordID(name, ID);
    dhash->retrieve(ID, wrap(&list_cb));

  } else if(!strcmp(cmd, "-f")) {
    // retrieve
    str2chordID(name, ID);
    dhash->retrieve(ID, wrap(&gotinode_cb));
    warnx << "retrieve " << ID << "\n";

  } else {
    fatal("filestore [sockname] -[fls] [filename/hash]\n");
  }

  amain();
  return 0;
}
