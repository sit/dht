#include "failuremodelfactory.h"
#include "nullfailuremodel.h"
#include "roundtripsfailuremodel.h"
#include "p2psim/args.h"

FailureModel *
FailureModelFactory::create(string s, vector<string>* v)
{
  Args *a = New Args(v);
  FailureModel *f = 0;

  if(s == "NullFailureModel")
    f = New NullFailureModel();

  if(s == "RoundtripsFailureModel")
    f = New RoundtripsFailureModel(a);

  return f;
}
