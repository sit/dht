#ifndef __PROT_FACTORY_H
#define __PROT_FACTORY_H

#include "p2psim/node.h"
#include "p2psim/args.h"
#include <set>
using namespace std;

class ProtocolFactory {
public:
  static ProtocolFactory* Instance();
  Protocol *create(string, Node*);
  void setprotargs(string, Args);
  set<string> getnodeprotocols();
  ~ProtocolFactory();

private:
  static ProtocolFactory *_instance;
  hash_map<string, Args> _protargs;
  set<string> _protocols;

  ProtocolFactory();
};

#endif // __PROT_FACTORY_H
