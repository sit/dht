#include "failuremodelfactory.h"
#include "nullfailuremodel.h"
#include "args.h"

FailureModel *
FailureModelFactory::create(string s, vector<string>* v)
{
  // Args *a = New Args(v);
  FailureModel *f = 0;

  if(s == "NullFailureModel")
    f = New NullFailureModel();

  return f;
}
