/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
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

#include "eventgenerator.h"
#include "p2psim/p2psim.h"
#include "parse.h"
#include "eventgenerators/eventgeneratorfactory.h"
#include "observers/observerfactory.h"
#include "libtask/task.h"
#include <vector>
#include <iostream>
#include <assert.h>
using namespace std;

void
EventGenerator::parse(char *filename)
{
  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    taskexitall(0);
  }

  string line;
  EventGenerator *gen = 0;
  Observer *obs = 0;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    //
    // generator
    //
    if(words[0] == "generator") {
      words.erase(words.begin());
      string generator = words[0];
      words.erase(words.begin());
      Args *a = New Args(&words);
      assert(a);
      if(!(gen = EventGeneratorFactory::Instance()->create(generator, a))) {
        cerr << "unknown generator " << generator << endl;
        exit(-1);
      }
      // spawn off this event generator thread
      gen->thread();

    //
    // observer
    //
    } else if(words[0] == "observer") {
      words.erase(words.begin());
      string observer = words[0];
      words.erase(words.begin());
      Args *a = New Args(&words);
      assert(a);
      if(!(obs = ObserverFactory::Instance()->create(observer, a))) {
        cerr << "unknown observer " << observer << endl;
        exit(-1);
      }
    } else {
      cerr << "first word of each line in event file should be ``generator'' or ``observer''" << endl;
      exit(-1);
    }
  }

  if(!gen) {
    cerr << "you should specify at least one event generator in the events file" << endl;
    exit(-1);
  }
}
