#ifndef __PROT_FACTORY_H
#define __PROT_FACTORY_H

#include "node.h"
#include <typeinfo>
// #include <map>

using namespace std;

class ProtocolFactory {
public:
  static ProtocolFactory* Instance();
  static void DeleteInstance();
  Protocol *create(string, Node*);
  string name(Protocol*);
  string name(const type_info &);

private:
  static ProtocolFactory *_instance;
  ProtocolFactory();
  ~ProtocolFactory();
  map<string, string> _protnames;
};

#endif // __PROT_FACTORY_H
