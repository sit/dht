/*
 *
 * Copyright (C) 2002  John Bicket (jbicket@mit.edu),
 *                     Sanjit Biswas (biswas@mit.edu)
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include "dhash.h"
#include "quorum.h"
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include "starcd.h"

enum quorum_read_state {
  QUORUM_READ_ASSEMBLING = 0,
  QUORUM_READ_FINISHED = 1,
  QUORUM_READ_NUM_STATES = 2
};

struct quorum_read_block_info{
  ptr<dhash_block> block;
  int count;
  ptr<quorum_read_block_info> next;
};

struct quorum_read_info {
  dhashclient dhash;
  bigint key;
  ptr<quorum_read_block_info> block_head;
  int num_failed;
  int num_success;
  int num_replicas;
  int state;
  cbretrieve_t cb;
  quorum_read_info(dhashclient d, cbretrieve_t c) : dhash(d), cb(c) { };
};

void quorum_start_read(ptr<quorum_read_info> ri);
void quorum_read_cb(ptr<quorum_read_info> ri, 
		    ptr<dhash_block> block);
void 
quorum_read (dhashclient dhash, bigint key, int replicas, cbretrieve_t cb)
{
  ptr<quorum_read_info> ri = New refcounted<quorum_read_info> (dhash, cb);
  ri->key = key;
  ri->num_failed = 0;
  ri->num_success = 0;
  ri->num_replicas = replicas;
  ri->block_head = NULL;
  ri->state = QUORUM_READ_ASSEMBLING;
  quorum_start_read(ri);
}

void quorum_start_read(ptr<quorum_read_info> ri) 
{
  bigint replica_key = ri->key;
  bigint temp;
  char key[sha1::hashsize];
  char result[sha1::hashsize];
  ri->num_failed = 0;
  ri->num_success = 0;
  ri->block_head = NULL;
  ri->state = QUORUM_READ_ASSEMBLING;
  int x;
  warn << "QUORUM_READ: starting retrieve for " << ri->num_replicas << " replicas\n";
  warn << "QUORUM_READ: key = " << ri->key << "\n";;
  for (x = 0; x < ri->num_replicas; x++) {
    // replica_key = sha1(key+x)
    // result = sha1(key)
    temp = ri->key + x;
    mpz_get_rawmag_be (key, sha1::hashsize, &temp); // For big endian
    sha1_hash(result, key, sha1::hashsize);
    mpz_set_rawmag_be (&replica_key, result, sizeof (result));
    warn <<"QUORUM_READ: replica "<< x << ", " <<temp<<"\n";
    ri->dhash.retrieve (replica_key, wrap (&quorum_read_cb, ri));
  }
  
}


/* compares the data of blocks. return 0 if they're the same. */
int blockcmp(dhash_block *a, dhash_block *b) {
  if (a->len != b->len) {
    return 1;
  }
  return memcmp(a->data, b->data, a->len);
}


void quorum_read_cb(ptr<quorum_read_info> ri, 
		    ptr<dhash_block> block)
{

  if (ri->state != QUORUM_READ_ASSEMBLING) {
    //warn << "QUORUM_READ_CB: wrong state: in "<<quorum_read_state_strings[ri->state]<<", wanted ASSEMBLING\n";
    return;
  }

  str errstr;
  if (NULL == block) {
    ri->num_failed++;
    warn <<"QUORUM_READ_CB: "<< ri->num_failed<<" responses failed\n";
    if (ri->num_failed > ri->num_replicas/3) {
      warn << "QUORUM_READ_CB: retrieve FAILED: " << ri->key << "\n";
      ri->state = QUORUM_READ_FINISHED;
      (*ri->cb) (NULL); // failure
    }
    return;
  }

  ri->num_success++;
  warn <<"QUORUM_READ_CB: block " << ri->key << "\n";
  warn << "QUORUM_READ_CB: " << ri->num_success<<" responses assembled\n";
  
  ptr<quorum_read_block_info> tmp = ri->block_head;


  /* have we seen teh block already? */
  while (NULL != tmp && !(0 == blockcmp(tmp->block, block))) {
    tmp = tmp->next;
  }

  if (NULL == tmp) {
    warn <<" new block\n";
    /* we got a block we haven't seen yet. */
    tmp = New refcounted<quorum_read_block_info>;
    tmp->block = block;
    tmp->count = 1;
    tmp->next = ri->block_head;
    ri->block_head = tmp;
  } else {
    warn << "seen block before\n";
    tmp->count++;
  }


  warn << "QUORUM_READ_CB: count is currently at "<<tmp->count << ", need " << ((ri->num_replicas/3) + 1) << "\n";
  
  if (tmp->count > ri->num_replicas/3) {
    warn << "QUORUM_READ_CB: returning callback\n";
    /* we've gotten enough repsonses to call the callback. */
    ri->state = QUORUM_READ_FINISHED;
    (*ri->cb) (block);
    return;
  }

  if (ri->num_success > 2*(ri->num_replicas/3)) {
    /* try again! */
    warn << "QUORUM_READ_CB: read couln't assemle enough responses,\n";
    ri->state = QUORUM_READ_ASSEMBLING;
    quorum_start_read(ri);
  }
  

}
