#ifndef __FAILURE_MODEL_FACTORY_H
#define __FAILURE_MODEL_FACTORY_H

#include "failuremodel.h"
#include <string>
using namespace std;

class FailureModelFactory {
public:
  static FailureModel *create(string s, vector<string>*);
};

#endif // __FAILURE_MODEL_FACTORY_H
