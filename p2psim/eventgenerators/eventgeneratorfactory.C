/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
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
 */

#include "eventgeneratorfactory.h"
#include "fileeventgenerator.h"
#include "churneventgenerator.h"

EventGeneratorFactory *EventGeneratorFactory::_instance = 0;

EventGeneratorFactory*
EventGeneratorFactory::Instance()
{
  if(!_instance)
    _instance = New EventGeneratorFactory();
  return _instance;
}

EventGeneratorFactory::EventGeneratorFactory()
{
}

EventGeneratorFactory::~EventGeneratorFactory()
{
}


EventGenerator *
EventGeneratorFactory::create(string type, vector<string> *v)
{
  EventGenerator *eg = 0;
  Args *a = New Args(v);
  assert(a);

  if(type == "FileEventGenerator")
    eg = New FileEventGenerator(a);

  if(type == "ChurnEventGenerator")
    eg = New ChurnEventGenerator(a);

  delete a;
  return eg;
}
