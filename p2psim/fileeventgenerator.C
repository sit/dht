#include "fileeventgenerator.h"
#include "protocolfactory.h"
#include "parse.h"
#include "event.h"
#include "eventfactory.h"
#include "eventqueue.h"
#include <fstream>
using namespace std;

FileEventGenerator::FileEventGenerator(vector<string> *v)
{
  Args args;
  args.convert(v);
  cout << "FileEventGenerator, name = " << args["name"] << endl;

  ifstream in(args["name"].c_str());
  if(!in) {
    cerr << "no such file " << args["name"] << ", did you supply the name parameter?" << endl;
    threadexitsall(0);
  }

  // register as observer of EventQueue
  EventQueue::Instance()->registerObserver(this);

  parse(in);
}


void
FileEventGenerator::parse(ifstream &in)
{
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
      EventQueue::Instance()->add_event(e);
    }
  }
}

void
FileEventGenerator::kick(Observed *)
{
}
