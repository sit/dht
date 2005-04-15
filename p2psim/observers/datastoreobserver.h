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

#ifndef DATAOBS_H
#define DATAOBS_H

#include "p2psim/observer.h"
#include "protocols/consistenthash.h"
#include "protocols/chord.h"

struct DataItem {
  Chord::CHID key;
  int size;
  DataItem (Chord::CHID k, int s) : key (k), size (s) {};
  DataItem () {};
};

class DataStoreObserver : public Observer {
public:
  DataStoreObserver(Args *a);

  virtual ~DataStoreObserver() {};
  virtual void kick(Observed *, ObserverInfo* = 0)  {};
  static DataStoreObserver* Instance(Args*);

  vector<DataItem> get_data();
  DataItem get_random_item ();

private:
  static DataStoreObserver *_instance;

  vector<DataItem> data_items;

};

#endif
