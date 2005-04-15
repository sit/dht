/*
 * Copyright (c) 2003-2005 Frank Dabek (fdabek@mit.edu)
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __DATASTORE_H
#define __DATASTORE_H

#include "p2psim/args.h"
#include "p2psim/node.h"
#include "p2psim/p2protocol.h"
#include "protocols/chordfingerpns.h"
#include "observers/datastoreobserver.h"

#include <assert.h>
#include <stdio.h>


struct fetch_args {
  Chord::CHID key;
};

struct fetch_res {
  bool present;
};

struct getdb_args {
  int max_keys;
};

struct getdb_res {
  vector<DataItem> keys;
};

struct senddata_args {
  vector<DataItem> keys;
};

struct senddata_res {
  bool ok;
};

class DataStore : public ChordFingerPNS {

public:

  DataStore (IPAddress i, Args& a, LocTable *l = NULL);

  virtual void initstate ();
  virtual void join(Args*);
  virtual void lookup (Args *);

  bool data_present (CHID k);
  int data_size (CHID k);
  void store (DataItem d);
  void remove (CHID k);

private:

  //configuration
  int _nreplicas;

  //stats
  long db_size;

  //"database:" maps a blockID to its size (if present)
  hash_map <CHID, DataItem> db;

  //state
  int _curr_succ;

  void stabilize_data (void *a);

  //handlers
  void fetch_handler (fetch_args *args, fetch_res *ret);
  void getdb_handler (getdb_args *args, getdb_res *res);
  void senddata_handler (senddata_args *args, senddata_res *res);

};

#endif /* _DATASTORE_H */
