#ifndef _DHASH_H_
#define _DHASH_H_
/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
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

#include <sys/time.h>

#include <qhash.h>

#include <arpc.h>
#include <async.h>

#include <chord_types.h>
#include <dhash_prot.h>
#include <route.h>

class dhash_block;
class dbrec;

/*
 * dhash.h
 *
 * Include file for the distributed hash service
 */


typedef callback<void, int, ptr<dbrec>, dhash_stat>::ptr cbvalue;
typedef callback<void,dhash_stat>::ptr cbstore;
typedef callback<void,dhash_stat>::ptr cbstat_t;
typedef callback<void, s_dhash_block_arg *>::ptr cbblockuc_t;
typedef callback<void, s_dhash_storecb_arg *>::ptr cbstorecbuc_t;
typedef callback<void, dhash_stat, ptr<dhash_block>, route>::ptr cb_ret;

extern unsigned int MTU;

class dhash {
 public:

  enum { NUM_EFRAGS = 14 };
  enum { NUM_DFRAGS = 7 };
  enum { SYNCTM = 30 };
  enum { KEYHASHTM = 10 };
  enum { REPTM = 3 };
  enum { PRTTM = 5 };


  // these 2 are only public for testing purposes
  virtual void replica_maintenance_timer (u_int index) = 0;
  virtual void partition_maintenance_timer () = 0;

  static ref<dhash> produce_dhash
    (str dbname, u_int nreplica = 0, int ss_mode = 0);

  virtual ~dhash () = 0;
  
  // XXX gross
  virtual void init_after_chord (ptr<vnode> node, ptr<route_factory> r_fact) = 0;

  virtual void print_stats () = 0;
  virtual void stop () = 0;
  virtual void fetch (chordID id, dhash_dbtype dbtype, int cookie, cbvalue cb) = 0;
  virtual void register_block_cb (int nonce, cbblockuc_t cb) = 0;
  virtual void unregister_block_cb (int nonce) = 0;
  virtual void register_storecb_cb (int nonce, cbstorecbuc_t cb) = 0;
  virtual void unregister_storecb_cb (int nonce) = 0;

  virtual dhash_stat key_status(const chordID &n) = 0;
};

// see dhash/server.C
extern int JOSH;

#endif
