#ifndef _DHASH_H_
#define _DHASH_H_
/*
 * Copyright (C) 2004  Emil Sit (sit@csail.mit.edu)
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

#include <arpc.h>
#include <async.h>

#include <chord_types.h>
#include <dhash_prot.h>

class location;
class vnode;
class dhash_block;
class blockID;

/*
 * dhash.h
 *
 * Include file for the distributed hash service
 */

struct dstat {
  str desc;
  u_int64_t value;
  dstat (str s, u_int64_t n) : desc (s), value (n) {};
};

typedef callback<void, str, int, int>::ptr cbfetch;
typedef callback<void, str, dhash_stat>::ptr cbvalue;
typedef callback<void,dhash_stat>::ptr cbstat_t;
typedef callback<void, s_dhash_block_arg *>::ptr cbblockuc_t;
typedef callback<void, dhash_stat, ptr<dhash_block>, vec<ptr<location> > >::ptr cb_ret;

class dhash {
 public:
  static u_long reptm ();
  
  static u_long dhash_disable_db_env ();

  static ref<dhash> produce_dhash (ptr<vnode> v, str dbname, cbv donecb);

  virtual ~dhash () = 0;

  virtual vec<dstat> stats () = 0;
  virtual void print_stats () = 0;
  
  virtual void start (bool randomize) = 0;
  virtual void stop () = 0;
  virtual void start_maint () = 0;

  virtual long register_fetch_callback (cbfetch cb) = 0;
  virtual void unregister_fetch_callback (long nonce) = 0;
};

#endif
