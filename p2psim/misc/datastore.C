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


#include "datastore.h"
#include "observers/datastoreobserver.h"
#include "observers/chordobserver.h"
#include "protocols/consistenthash.h"
#include "p2psim/p2psim_hashmap.h"
#include <iostream>

using namespace std;

DataStore::DataStore (IPAddress i, Args& a, LocTable *l /* = NULL, see .h*/) :
  ChordFingerPNS(i, a, l)
{
  _nreplicas = a.nget<uint>("replicas", 3, 10);
}


void
DataStore::initstate ()
{
  ChordFingerPNS::initstate();
  
  //find the range of keys we should be storing using global knowledge
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  IDMap pred;
  int index = -1;
  assert (ids.size () > 0);
  for (int i = ids.size () - 1; i >= 0; i--)
    {
      //found ourselves,  
      if (ids[i].id == me.id) {
	//count back _nreplicas
	index = i - _nreplicas;
	if (index < 0) index = ids.size () + index;
	pred = ids[index];
      }
      
    }
  assert (index >= 0);

  cerr << me.id << ": storing items between " << pred.id << 
    " and " << me.id << "\n";
  vector<DataItem> vd = DataStoreObserver::Instance(NULL)->get_data();
  for (unsigned int i = 0; i < vd.size (); i++) 
    {
      DataItem d = vd[i];
      if (ConsistentHash::between (pred.id, me.id, d.key))
	store (d);
    }
}

void 
DataStore::join (Args* a)
{

  //start stabilization for data
  int start = random () % 5000;
  delaycb (start, &DataStore::stabilize_data, (void *) 0);
  ChordFingerPNS::join (a);
}

void
DataStore::fetch_handler (fetch_args *args, fetch_res *ret)
{
  if (data_present (args->key))
    ret->present = true;
  else
    ret->present = false;
}

void
DataStore::lookup (Args *args)
{
  lookup_args *a = New lookup_args;
  bzero (a, sizeof (lookup_args));
  a->key = args->nget<CHID>("key");
  a->start = now();

  IDMap lasthop;
  vector<IDMap> v;
  if (_recurs)
    v = find_successors_recurs(a->key, _nreplicas,
				     TYPE_USER_LOOKUP, &lasthop, a);
  else
    v = find_successors (a->key, _nreplicas, 
				     TYPE_USER_LOOKUP, &lasthop, a);

  if (!alive()) return;

  if (v.size() > 0) {
    bool done = false;
    int nsucc = 0;
    while (!done) {
      IDMap succ = v[nsucc];

      //send an RPC to get the data
      // we should really have it returned directly
      fetch_args args;
      args.key = a->key;
      fetch_res res;
      res.present = false;
      doRPC (succ.ip, &DataStore::fetch_handler, &args, &res);
      if (!alive()) break; 
      
      if (res.present) {
	cerr << ip () << " " << now () << " : data object " << a->key 
	     << " found successfully at node " << succ.id << " ("
	     << nsucc << ")\n";
	record_lookup_stat (me.ip, succ.ip, now () - a->start, true, true);
	done = true;
      }  else {
	nsucc++;
	if (nsucc >= (uint) _nreplicas  || nsucc >= (uint) v.size ()) {
	  cerr << ip () << ": error looking up " << a->key << " "
	       << nsucc << " " << _nreplicas << " " << v.size () << "\n";
	  record_lookup_stat (me.ip, succ.ip, now () - a->start, true, false);
	  done = true;
	}
      }
    }
  }

  delete a;
}

void 
DataStore::stabilize_data (void *a)
{

  if (!alive() || _nreplicas < 2) return; 

  cerr << ip () << " " << now () << " starting stabilize\n";


  //merkle-like scheme
  vector<IDMap> succs = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);

  //get a successor's database
  getdb_args args;
  getdb_res res;
  doRPC (succs[_curr_succ].ip, &DataStore::getdb_handler, &args, &res);

  if (!alive ()) return;

  senddata_args sd_arg;
  senddata_res sd_res;
  //make sure that he has all of the keys that we do
  hash_map<CHID, DataItem>::iterator it = db.begin ();
  for (; it != db.end (); ++it) {
    uint i = 0;
    for (; i < res.keys.size (); i++)
    {
      //      cerr << "comparing " << (*it).first << " and " << res.keys[i].key << "\n";
      if ((*it).first == res.keys[i].key) break;
    }
    if (i == res.keys.size ()) {
      //add it to the database
      cerr << "sending " << (*it).first 
	   << " to succ # " << _curr_succ << "\n";
      sd_arg.keys.push_back ((*it).second);
    }
  }
  
  //send the message
  if (sd_arg.keys.size () > 0) {
    cerr << "sending " << sd_arg.keys.size () << " keys to succ # " 
	 << _curr_succ << " of " << db.size () << "\n";
    doRPC (succs[_curr_succ].ip, &DataStore::senddata_handler, &sd_arg, &sd_res);

    if (!alive ()) return;
  }

  
  _curr_succ++;
  if (_curr_succ >= _nreplicas - 1) _curr_succ = 0;
  int start = random () % 5000;
  delaycb (start, &DataStore::stabilize_data, (void *) 0);
}

void
DataStore::senddata_handler (senddata_args *args, senddata_res *res)
{
  for (uint i = 0; i < args->keys.size (); i++)
    {
      store (args->keys[i]);
    }
}

void
DataStore::getdb_handler (getdb_args *args, getdb_res *res)
{
  hash_map<CHID, DataItem>::iterator i = db.begin ();
  for (; i != db.end (); ++i)
    {
      res->keys.push_back ((*i).second);
    }

  cerr << ip () << " getdb_handler: returned " << res->keys.size () << " keys of "
       << db.size () << "\n";
}
//----- database routines

void
DataStore::store (DataItem d)
{
  db[d.key] = d;
  db_size += d.size;
}

void
DataStore::remove (CHID k)
{
  if (data_present (k)) {
    hash_map<CHID, DataItem>::iterator i = db.find (k);
    db.erase (i);
  }
}

bool
DataStore::data_present (CHID k) 
{
  return (db.find (k) != db.end ());
}

int
DataStore::data_size (CHID k)
{
  if (data_present (k))
    return db[k].size;
  else 
    return -1;
}
