/*
 * Copyright (c) 2003 Frank Dabek (fdabek@mit.edu)
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
#include "protocols/consistenthash.h"
#include "hash_map.h"

void
DataStore::initstate ()
{
  ChordFingerPNS::initstate();

  vector<DataItem> vd = DataStoreObserver::Instance(NULL)->get_data();

  for (unsigned int i = 0; i < vd.size (); i++) 
    {
      DataItem d = vd[i];
      IDMap pred = loctable->pred(me.id);
      if (ConsistentHash::between (pred.id, me.id, d.key))
	store (d);
    }
}

void 
DataStore::join (Args* a)
{

  //start stabilization for data
  // delaycb (1000, &DataStore::stabilize_data, (void *) 0);

  ChordFingerPNS::join (a);
}

void
DataStore::lookup (Args *args)
{
  lookup_args *a = New lookup_args;
  bzero (a, sizeof (lookup_args));
  a->key = args->nget<CHID>("key");
  a->start = now();

  IDMap lasthop;
  vector<IDMap> v = find_successors (a->key, _frag, 
				     TYPE_USER_LOOKUP, &lasthop, a);

  if (v.size() > 0) {
    IDMap succ = v[0];
    DataStore *succ_node = (DataStore *)getpeer(succ.ip);
    if (succ_node->data_present (a->key))
      cerr << ip () << ": data object " << a->key 
	   << " found successfully at node " << succ.id << "\n";
    else 
      cerr << ip () << ": error looking up " << a->key << "\n";
    
  }

  delete a;
}

void 
DataStore::stabilize_data (void *a)
{

}


//----- database routines

void
DataStore::store (DataItem d)
{
  db[d.key] = d;
  db_size += d.size;
  cerr << me.id << ": storing " << d.key << "\n";
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
