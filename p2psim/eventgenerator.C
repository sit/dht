#include "eventgenerator.h"
#include "parse.h"
#include "eventgeneratorfactory.h"
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
      cerr << "first line in event file should be ``generator [G]''" << endl;
      exit(-1);
    }
    words.erase(words.begin());

    string generator = words[0];
    words.erase(words.begin());
    gen = EventGeneratorFactory::Instance()->create(generator, &words);
    break;
  }

  if(!gen) {
    cerr << "the generator you specified is unknown" << endl;
    exit(-1);
  }

  // leave the rest of the file to the specific generator
  gen->parse(in);

  return gen;
}
