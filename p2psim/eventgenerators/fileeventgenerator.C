/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu)
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

#include "fileeventgenerator.h"
#include "protocols/protocolfactory.h"
#include "events/eventfactory.h"
#include "p2psim/eventqueue.h"
#include <fstream>
using namespace std;

FileEventGenerator::FileEventGenerator(Args *args)
{
  // cout << "FileEventGenerator, name = " << (*args)["name"] << endl;
  _name = (*args)["name"];
  EventQueue::Instance()->registerObserver(this);
}


void
FileEventGenerator::run()
{
  ifstream in(_name.c_str());
  if(!in) {
    cerr << "no such file " << _name << ", did you supply the name parameter?" << endl;
    threadexitsall(0);
  }

  string line;

  set<string> allprotos = ProtocolFactory::Instance()->getnodeprotocols();
  if (allprotos.size() > 1) {
    cerr << "warning: running two diffferent protocols on the same node" << endl;
  }

  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // create the appropriate event type (based on the first word of the line)
    // and let that event parse the rest of the line
    string event_type = words[0];
    words.erase(words.begin());
    for (set<string>::const_iterator i = allprotos.begin(); i != allprotos.end(); ++i) {
      Event *e = EventFactory::Instance()->create(event_type, &words, *i);
      assert(e);
      add_event(e);
    }
  }

  EventQueue::Instance()->go();
}

void
FileEventGenerator::kick(Observed *o, ObserverInfo* oi)
{
}
