#ifndef _DHASH_COMMON_H_
#define _DHASH_COMMON_H_

#include <chord_types.h>
#include <dhash_prot.h>
#include <crypt.h>

#define DHASHCLIENT_GUESS_SUPPLIED 0x1
#define DHASHCLIENT_NO_RETRY_ON_LOOKUP 0x2
#define DHASHCLIENT_USE_CACHE 0x4
#define DHASHCLIENT_CACHE 0x8

struct blockID {
  enum { size = sha1::hashsize };

  chordID ID;
  dhash_ctype ctype;
  dhash_dbtype dbtype;

  blockID (chordID k, dhash_ctype c, dhash_dbtype d)
    : ID(k), ctype(c), dbtype(d)
  {} 

  bool operator== (const blockID b) const {
    return((ID == b.ID) &&
	   (ctype == b.ctype) &&
	   (dbtype == b.dbtype));
  }
};

inline const strbuf &
strbuf_cat (const strbuf &sb, const blockID &b)
{
  return sb << b.ID << ":" << (int)b.ctype << ":" << (int)b.dbtype;
}

struct bhashID {
  bhashID () {}
  hash_t operator() (const blockID &ID) const {
    return ID.ID.getui ();
  }
};

struct dhash_block {
  chordID ID;
  dhash_ctype ctype;
  char *data;
  size_t len;
  long version;
  int hops;
  int errors;
  int retries;
  vec<u_long> times;
  chordID source;

  ~dhash_block () {  delete [] data; }

  dhash_block (const char *buf, size_t buflen, dhash_ctype ct)
    : ctype(ct), data (New char[buflen]), len (buflen)
  {
    if (buf)
      memcpy (data, buf, len);
  }
};

struct insert_info { 
  chordID key;
  vec<chordID> path;
  insert_info (chordID k, vec<chordID> p) :
    key (k), path (p) {};
};

static inline str dhasherr2str (dhash_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}

#endif /* _DHASH_COMMON_ */
