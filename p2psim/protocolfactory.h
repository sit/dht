#ifndef __PROT_FACTORY_H
#define __PROT_FACTORY_H

#include "node.h"
#include "args.h"
#include <typeinfo>
#include <set>
#include <map>
#include <string>

using namespace std;

class ProtocolFactory {
public:
  static ProtocolFactory* Instance();
  static void DeleteInstance();
  Protocol *create(string, Node*);
  void setprotargs(string, Args);
  set<string> getnodeprotocols();

private:
  static ProtocolFactory *_instance;
  map<string, Args> _protargs;
  set<string> _protocols;

  ProtocolFactory();
  ~ProtocolFactory();
};

#endif // __PROT_FACTORY_H
