/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
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
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "eventgeneratorfactory.h"
#include "fileeventgenerator.h"
#include "churneventgenerator.h"
#include "churnfileeventgenerator.h"
#include "sillyeventgenerator.h"

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
EventGeneratorFactory::create(string type, Args *a)
{
  EventGenerator *eg = 0;

  if(type == "FileEventGenerator")
    eg = New FileEventGenerator(a);

  if(type == "ChurnFileEventGenerator")
    eg = New ChurnFileEventGenerator(a);

  if(type == "ChurnEventGenerator")
    eg = New ChurnEventGenerator(a);

  if(type == "SillyEventGenerator")
    eg = New SillyEventGenerator(a);

  delete a;
  return eg;
}
