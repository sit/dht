#include <err.h>
#include <str.h>
#include <parseopt.h>
#include "configurator.h"

Configurator::Configurator () : parsed_ (false)
{
}

Configurator::~Configurator ()
{
  ConfigPair *c = NULL, *n = NULL;
  c = conf.first ();
  while (c) {
    n = conf.next (c);
    delete c;
    c = n;
  }
}

Configurator &
Configurator::only ()
{
  // store a ptr object so that this will get destructed.
  static ptr<Configurator> instance = NULL;
  if (instance == NULL)
    instance = New refcounted<Configurator> ();
  return *instance;
}

bool
Configurator::parse (const char *filename)
{
  bool errors = false;
  if (parsed_)
    return false;

  parseargs pa (filename);
  
  int line;
  vec<str> av;
  
  while (pa.getline (&av, &line)) {
    if ('#' == av[0][0])
      continue;

    if (av.size () < 2) {
      warn << filename << ":" << line << ": Line too short.\n";
      errors = true;
      continue;
    }
    
    ConfigPair *n = conf.search (av[0]);
    int vali;
    bool isint = convertint (av[1], &vali);
    if (n) {
      // warn << filename << ":" << line << ": redefines `" << av[0] << "'\n";
      if (isint) {
	n->set_int (vali);
      } else {
	n->set_str (av[1]);
      }
    } else {
      if (isint) 
	n = New ConfigPair (av[0], vali);
      else
	n = New ConfigPair (av[0], av[1]);
      conf.insert (n);
    }
  }

  if (errors)
    fatal ("parse errors in configuration file\n");
  
  return (parsed_ = true);
}

bool
Configurator::parsed () const
{
  return parsed_;
}

bool
Configurator::get_int (const char *field, int &resp) const
{
  ConfigPair *n = conf.search (field);
  if (!n || n->type_ != ConfigPair::INT) {
    warnx << "Configurator::get_int: unknown field " << field << "\n";
    return false;
  }
  resp = n->vali_;
  return true;
}


bool
Configurator::get_str (const char *field, str &resp) const
{
  ConfigPair *n = conf.search (field);
  if (!n || n->type_ != ConfigPair::STRING) {
    warnx << "Configurator::get_str: unknown field " << field << "\n";
    return false;
  }
  resp = n->vals_;
  return true;
}

bool
Configurator::set_str (const char *field, str newv)
{
  ConfigPair *n = conf.search (field);
  if (n)
    n->set_str (newv);
  else {
    n = New ConfigPair (field, newv);
    conf.insert (n);
  }
  return true;
}


bool
Configurator::set_int (const char *field, int newv)
{
  ConfigPair *n = conf.search (field);
  if (n)
    n->set_int (newv);
  else {
    n = New ConfigPair (field, newv);
    conf.insert (n);
  }
  return true;
}

void
Configurator::dump ()
{
  ConfigPair *n = conf.first ();
  warnx << "=== Configurator::dump\n";
  while (n) {
    strbuf x;
    x << n->var_ << " ";
    switch (n->type_) {
    case ConfigPair::INT:
      x << n->vali_;
      break;
    case ConfigPair::STRING:
      x << "\"" << n->vals_ << "\"";
      break;
    default:
      x << "ERROR";
      break;
    }
    x << "\n";
    warnx << x;
    n = conf.next (n);
  }
}
