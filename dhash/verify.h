
#ifndef __DHASH_VERIFY_H__
#define __DHASH_VERIFY_H__

#include <refcnt.h>

enum dhash_ctype;
class dbrec;

/* verify.C */
bool verify (chordID key, dhash_ctype t, char *buf, int len);
bool verify_content_hash (chordID key,  char *buf, int len);
bool verify_keyhash (chordID key, char *buf, int len);
ptr<dhash_block> get_block_contents (ref<dbrec> d, dhash_ctype t);
ptr<dhash_block> get_block_contents (ptr<dbrec> d, dhash_ctype t);
ptr<dhash_block> get_block_contents (ptr<dhash_block> block, dhash_ctype t);
ptr<dhash_block> get_block_contents (char *data, 
				     unsigned int len, 
				     dhash_ctype t);

long keyhash_version (ptr<dbrec> data);
long keyhash_version (ref<dbrec> data);
long keyhash_version (ptr<dhash_block> data);
long keyhash_version (ref<dhash_block> data);
long keyhash_version (char *value, unsigned int len);

bigint compute_hash (const void *buf, size_t buflen);

#endif

