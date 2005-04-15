/*
 * Copyright (c) 2003-2005 Frank Dabek
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

#include "p2psim/node.h"
#include "p2psim/args.h"
#include "p2psim/network.h"
#include "protocols/protocolfactory.h"
#include "datastoreobserver.h"

#include <iostream>
#include <set>
#include <algorithm>
#include <stdio.h>


DataStoreObserver* DataStoreObserver::_instance = 0;

DataStoreObserver*
DataStoreObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New DataStoreObserver(a);
  return _instance;
}


DataStoreObserver::DataStoreObserver(Args *a)
{
  _instance = this;

  //pick data items / sizes
  // XXX get number of items from args
  for (int i = 0; i < 1000; i++) 
    {
      Chord::CHID id = ConsistentHash::getRandID ();
      int size = 8192;
      data_items.push_back (DataItem (id, size));
    }

  // XXX sort it?
}

DataItem
DataStoreObserver::get_random_item ()
{    
  int r = random () % data_items.size ();
  return data_items[r];
}

vector<DataItem>
DataStoreObserver::get_data()
{

  assert(data_items.size()>0);
  return data_items;
}



