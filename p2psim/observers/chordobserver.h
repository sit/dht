/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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
 */

#ifndef __CHORD_OBSERVER_H
#define __CHORD_OBSERVER_H

#include "protocols/chord.h"
#include "p2psim/oldobserver.h"

class ChordObserver : public Oldobserver {
public:
  static ChordObserver* Instance(Args*);
  virtual void execute();
  vector<Chord::IDMap> get_sorted_nodes(unsigned int = 0);

private:
  void init_nodes(unsigned int num);
  void vivaldi_error();
  static ChordObserver *_instance;
  ChordObserver(Args*);
  ~ChordObserver();
  unsigned int _reschedule;
  unsigned int _num_nodes;
  unsigned int _init_num;

  vector<ConsistentHash::CHID> lid;
  vector<Chord::IDMap> _allsorted;
};

#endif // __CHORD_OBSERVER_H
