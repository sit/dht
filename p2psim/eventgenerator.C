#include "eventgenerator.h"
#include "parse.h"
#include "eventgeneratorfactory.h"
#include <lib9.h>
#include <thread.h>
#include <vector>
using namespace std;

EventGenerator*
EventGenerator::parse(char *filename)
{
  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    threadexitsall(0);
  }

  string line;
  EventGenerator *gen = 0;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // read generator string
    if(words[0] != "generator") {
      cerr << "first word of each line in event file should be ``generator''" << endl;
      exit(-1);
    }
    words.erase(words.begin());

    string generator = words[0];
    words.erase(words.begin());
    if(!(gen = EventGeneratorFactory::Instance()->create(generator, &words))) {
      cerr << "unknown generator " << generator << endl;
      exit(-1);
    }

    // spawn off this event generator thread
    gen->thread();
  }

  if(!gen) {
    cerr << "you should specify at least one generator" << endl;
    exit(-1);
  }

  return gen;
}
