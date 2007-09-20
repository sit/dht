#ifndef _DHASH_COMMON_H_
#define _DHASH_COMMON_H_

#include <chord_types.h>
#include <dhash_prot.h>
#include <crypt.h>

#define DHASHCLIENT_GUESS_SUPPLIED 0x1
#define DHASHCLIENT_NO_RETRY_ON_LOOKUP 0x2
// pls leave these two options here, proxy server currently uses them
// - benjie
#define DHASHCLIENT_USE_CACHE 0x4
#define DHASHCLIENT_CACHE 0x8
#define DHASHCLIENT_SUCCLIST_OPT 0x10
#define DHASHCLIENT_NEWBLOCK 0x20
#define DHASHCLIENT_RMW 0x40
#define DHASHCLIENT_SKIP_LOOKUP 0x80
#define DHASHCLIENT_EXPIRATION_SUPPLIED 0x100

struct blockID {
  chordID ID;
  dhash_ctype ctype;

  blockID (chordID k, dhash_ctype c) : ID (k), ctype (c) {};
  blockID (const blockID &b) : ID (b.ID), ctype (b.ctype) {};

  bool operator== (const blockID b) const {
    return ((ID == b.ID) &&
	    (ctype == b.ctype));
  }

  blockID& operator= (const blockID &b) {
    if( this != &b ) {
      ID = b.ID;
      ctype = b.ctype;
    }
    return *this;
  }

};

inline const strbuf &
strbuf_cat (const strbuf &sb, const blockID &b)
{
  return sb << b.ID << ":" << (int)b.ctype;
}

struct bhashID {
  bhashID () {}
  hash_t operator() (const blockID &ID) const {
    return ID.ID.getui ();
  }
};

struct dhash_block 
{
  chordID ID;
  dhash_ctype ctype;
  str data;
  vec<str> vData;
  u_int32_t expiration;
  int hops;
  int errors;
  int retries;
  vec<u_long> times;
  chordID source;

  ~dhash_block () { }

  dhash_block (const char *buf, size_t buflen, dhash_ctype ct)
    : ctype(ct), data (buf, buflen) { };

  dhash_block (str data, dhash_ctype ct)
    : ctype(ct), data (data) { };
};

static inline str dhasherr2str (dhash_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}
inline const strbuf &
strbuf_cat (const strbuf &sb, dhash_stat status)
{
  return rpc_print (sb, status, 0, NULL, NULL);
}

#endif /* _DHASH_COMMON_ */
