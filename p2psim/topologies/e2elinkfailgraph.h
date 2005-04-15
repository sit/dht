/*
 * Copyright (c) 2003-2005 Jeremy Stribling
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

#ifndef __E2ELINKFAILGRAPH_H
#define __E2ELINKFAILGRAPH_H

#include <string>
#include <vector>
#include "p2psim/topology.h"

using namespace std;


class E2ELinkFailGraph : public Topology {

	public:
	E2ELinkFailGraph(vector<string>*);
	~E2ELinkFailGraph();

	virtual void parse(ifstream&);
	virtual Time latency(IPAddress, IPAddress, bool = false);

	private:
	unsigned int _num_nodes;
	unsigned int _num_samples;
	vector<vector<Time> > _pairwise;
	vector<vector<vector<bool> > > _link_working;
};

#endif // __E2ELINKFAILGRAPH_H
