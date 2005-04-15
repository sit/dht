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

#ifndef __VIVALDITEST_H
#define __VIVALDITEST_H

#include "p2psim/p2protocol.h"
#include "vivaldinode.h"

class VivaldiTest : public VivaldiNode {
public:
  VivaldiTest(IPAddress i, Args &args);
  ~VivaldiTest();
  string proto_name() { return "VivaldiTest"; }

  virtual void join(Args*);
  virtual void leave(Args*) { }
  virtual void crash(Args*) { }
  virtual void insert(Args*) { }
  virtual void lookup(Args*) { }
  virtual void nodeevent (Args *);

  virtual void initstate ();
  void tick(void *);
  void status();
  double error();
  vector<IPAddress> best_n (unsigned int n);
  void total_error(double &x05, double &x50, double &x95);
  void node_errors(double &x05, double &x50, double &x95);
  vector<IPAddress> _nip_best;
  vector<IPAddress> _nip;
  vector<IPAddress> _nip_far;
  int _neighbors_far;

 private:
  int _ticks;
  int _grid_config;
  int _ring_config;
  int _landmark_config;
  int _near_config;

  bool _aux_added; 
  //some neighbor adding strategies
  // need to run after _all is filled
  // in. This flag lets us run them once

  Vivaldi *_vivaldi;
  static vector<VivaldiTest*> _all;

  int _next_neighbor;
  int _neighbors; // if > 0, fix the number of neighbors
  uint _total_nodes;
  int _vis;
  double _last_error;
  bool _joined;
  int _far_percent;

  uint _old_all_size;

  void handler(void *, void *);

  void addRingNeighbors ();
  void addGridNeighbors ();
  void addRandNeighbors ();
  void addLandmarkNeighbors ();
  void addNearNeighbors ();
  void print_all_loc();
};

#endif // __VIVALDITEST_H
