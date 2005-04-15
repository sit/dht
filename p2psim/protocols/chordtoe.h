/*
 * Copyright (c) 2003-2005 Jinyang Li
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

#ifndef __CHORDTOE_H
#define __CHORDTOE_H

#include "chordfinger.h"
#include "chordfingerpns.h"

/* ChordFinger implements finger table in addition to Chord with succ list*/

class LocTableToe : public LocTable {
public:
  int _style;
  vector<Chord::IDMap> _toes;

  LocTableToe () : LocTable () {};
  virtual ~LocTableToe () {};

  Chord::IDMap next_hop(Chord::CHID key);

  void set_style (int i) {_style = i;};
  void set_toes (vector<Chord::IDMap> t) { _toes = t; };
};

class ChordToe : public ChordFingerPNS {
public:
  ChordToe(IPAddress i, Args& a);
  ChordToe ();
  ~ChordToe() {};
  string proto_name() { return "ChordToe"; }
  
  void dump() {};
  bool stabilized (vector<CHID> lid) { return true;}
  void initstate();
  
protected:

  int _lookup_style;
  
  int add_toe (IDMap i);

  vector<IDMap> _toes;
  uint _numtoes;
  
};

#endif
