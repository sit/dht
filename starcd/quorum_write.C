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

#include "starcd.h"
#include "dhash.h"
#include "quorum.h"
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"

enum quorum_write_state {
  QUORUM_WRITE_RESERVE = 0,
  QUORUM_WRITE_COMMIT = 1,
  QUORUM_WRITE_FINISHED = 2,
  QUORUM_WRITE_NUM_STATES = 3
};

char *quorum_write_state_strings[QUORUM_WRITE_NUM_STATES] = {"reserve", "commit", "finished"};

struct quorum_write_info {
  dhashclient dhash;
  bigint key;
  int state;
  int insert_state;
  int num_replicas;
  size_t credlen;
  size_t datalen;
  const char *cred;
  const char *data;
  cbinsertgw_t cb;
  int reserved_success;
  int reserved_failed;
  int committed;
  int failed;
  quorum_write_info(dhashclient d, cbinsertgw_t c) : dhash(d), cb(c) { };
};

void quorum_reserve_cb (ptr <quorum_write_info> si, 
			dhash_stat status, 
			ptr<insert_info> i);
void quorum_start_reserve (ptr<quorum_write_info> si);
void quorum_commit (ptr<quorum_write_info> si);
void quorum_commit_cb (ptr<quorum_write_info> si, 
		       dhash_stat status, 
		       ptr<insert_info> i);

// quorum_write for stafish blocks
void quorum_write(dhashclient dhash, bigint key, int replicas,
			 const char *cred, size_t credlen,
			 const char *data, size_t datalen, 
			 cbinsertgw_t cb) {
  warn << "QUORUM_WRITE: beginning quorum_write for "<<key<<"\n";
  ptr<quorum_write_info> si = New refcounted<quorum_write_info> (dhash, cb);
  si->key = key;
  si->num_replicas = replicas;
  si->credlen = credlen;
  si->datalen = datalen;
  si->cred = cred;
  si->data = data;
  si->reserved_success = 0;
  si->reserved_failed = 0;
  si->committed = 0;
  si->failed = 0;
  si->state = QUORUM_WRITE_RESERVE;
  warn << "will need " << ((si->num_replicas *2) / 3) << " for reservation";
  warn << " or " << (si->num_replicas / 3) << " to fail\n";
  si->state = QUORUM_WRITE_COMMIT;
  warn <<"QUORUM: COMMITTING NOW "<<si->key<<"\n";
  quorum_commit(si);
  return;
  //quorum_start_reserve(si);
}

void quorum_start_reserve(ptr<quorum_write_info> si) 
{
  bigint replica_key = si->key;
  bigint temp;
  char key[sha1::hashsize];
  char result[sha1::hashsize];
  int x;
  si->state = QUORUM_WRITE_RESERVE;
  si->reserved_success = 0;
  si->reserved_failed = 0;
  warn << "QUORUM_WRITE: starting reservation: " << si->key <<" need "<< 2*(si->num_replicas/3)<<"/"<<si->num_replicas<< " replicas\n";
  for (x = 0; x < si->num_replicas; x++) {
    // replica_key = sha1(key+x)
    // result = sha1(key)
    temp = si->key + x;
    mpz_get_rawmag_be (key, sha1::hashsize, &temp); // For big endian
    sha1_hash(result, key, sha1::hashsize);
    mpz_set_rawmag_be (&replica_key, result, sizeof (result));
    //warn <<"QUORUM_RESERVE: "<<replica_key<<"\n";
    si->dhash.insert(si->dhash, 
		     si->key, 
		     replica_key, 
		     si->cred, si->credlen,
		     si->data, si->datalen,		  
		     QUORUM_RESERVE, wrap(&quorum_reserve_cb, si));
  }
  
}
void quorum_reserve_cb(ptr <quorum_write_info> si, dhash_stat status, ptr<insert_info> i) 
{
  if (si->state != QUORUM_WRITE_RESERVE) {
    //warn << "QUORUM_RESERVE_CALLBACK: wrong state: in "<<quorum_write_state_strings[si->state]<<", wanted RESERVE\n";
    return;
  }
  if (status == DHASH_OK) {
    si->reserved_success++;
    //warn <<"QUORUM_RESERVE_CALLBACK: "<< si->reserved_success<<" succeeded\n";
  } else {
    si->reserved_failed++;
    warn <<"QUORUM_RESERVE_CALLBACK: "<< si->reserved_failed<<" failed\n";
	
  }

  if (si->reserved_success > ((si->num_replicas * 2) / 3)) {
    /* ok we can commit now! */
    si->state = QUORUM_WRITE_COMMIT;
    warn <<"QUORUM: COMMITTING NOW "<<si->key<<"\n";
    quorum_commit(si);
    return;
  } else if (si->reserved_failed > (si->num_replicas / 3)) {
    //int backoff = random() % 3;
    /* back off, buddy */
    //si->state = QUORUM_WRITE_BACKOFF;
    //warn << "QUORUM: backing off quorum_write for "<<backoff<<"\n";
    //delaycb(backoff, wrap(this, &quorum_start_reserve, si));
    /* fail instead of backing off */
    warn <<"STASRFISH_COMMIT: reservation failed\n";
    si->state = QUORUM_WRITE_FINISHED;
    ptr<insert_info> i = New refcounted<insert_info>(si->key, bigint(0));
    si->cb(DHASH_ERR, i);
  } 
}

void quorum_commit(ptr<quorum_write_info> si) 
{
  bigint replica_key = si->key;
  bigint temp;
  char key[sha1::hashsize];
  char result[sha1::hashsize];
  int x;
  warn << "QUORUM_COMMIT: key " << replica_key <<"\n";
  for (x = 0; x < si->num_replicas; x++) {
    // the ideas is that:
    // replica_key = sha1(key+x)
    // result = sha1(key)
    temp = si->key + x;
    mpz_get_rawmag_be (key, sha1::hashsize, &temp); // For big endian
    sha1_hash(result, key, sha1::hashsize);
    mpz_set_rawmag_be (&replica_key, result, sizeof (result));
    //warn <<"QUORUM_COMMIT: replica "<<temp<<"\n";
    si->dhash.insert(si->dhash,
		     si->key,
		     replica_key, 
		     si->cred, si->credlen,
		     si->data, si->datalen,		  
		     QUORUM_COMMIT, wrap(&quorum_commit_cb, si));
  }
}
void quorum_commit_cb(ptr<quorum_write_info> si, dhash_stat status, ptr<insert_info> i) 
{
  warn << "QUORUM_COMMIT_CB: in "<<quorum_write_state_strings[si->state]<<"\n";

  if (si->state != QUORUM_WRITE_COMMIT) {
    //warn << "QUORUM_COMMIT_CB: wrong state: in "<<quorum_write_state_strings[si->state]<<", wanted COMMIT\n";
    return;
  }
  if (status == DHASH_OK) {
    si->committed++;
    warn <<"QUORUM_COMMIT_CALLBACK: "<< si->committed<<" committed\n";
  } else {
    si->failed++;
    warn <<"AAAAAHHHH COMMIT FAILED!!!\n";
    /* ??? */
  }
  if (si->failed > si->num_replicas/3) {
    warn <<"QUORUM_COMMIT_CALLBACK: failure for " <<si->key<<"\n";
    si->state = QUORUM_WRITE_FINISHED;
    /* we're done :( */
    ptr<insert_info> i = New refcounted<insert_info>(si->key, bigint(0));
    si->cb(DHASH_ERR, i);

  } else if (si->committed > 2*(si->num_replicas/3)) {
    warn <<"QUORUM_COMMIT_CALLBACK: success for "<<si->key<<"\n";
    si->state = QUORUM_WRITE_FINISHED;
    /* we're done! */
    ptr<insert_info> i = New refcounted<insert_info>(si->key, bigint(0));
    si->cb(DHASH_OK, i);
  }
}
