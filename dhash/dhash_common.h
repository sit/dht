#ifndef _DHASH_COMMON_H_
#define _DHASH_COMMON_H_

#include <chord_types.h>
#include <dhash_prot.h>

#define DHASHCLIENT_USE_CACHED_SUCCESSOR 0x1
#define DHASHCLIENT_NO_RETRY_ON_LOOKUP   0x2

struct dhash_block {
  chordID ID;
  char *data;
  size_t len;
  long version;
  int hops;
  int errors;
  int retries;
  vec<u_long> times;
  chordID source;

  ~dhash_block () {  delete [] data; }

  dhash_block (const char *buf, size_t buflen)
    : data (New char[buflen]), len (buflen)
  {
    if (buf)
      memcpy (data, buf, len);
  }
};

struct insert_info { 
  chordID key;
  chordID destID;
  insert_info (chordID k, chordID d) :
    key (k), destID (d) {};
};

static inline str dhasherr2str (dhash_stat status)
{
  return rpc_print (strbuf (), status, 0, NULL, NULL);
}

#endif /* _DHASH_COMMON_ */
