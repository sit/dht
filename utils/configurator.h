#ifndef __CONFIGURATOR_H__
#define __CONFIGURATOR_H__ 1

#include <refcnt.h>
#include "skiplist.h"

class str;
/*
 * The Configurator is a singleton object for handling configuration information
 * that is loosely based on sysctl interface in FreeBSD.  The format of the
 * configuration file a list of variable/value pairs.
 */
class Configurator {
 private:
  struct ConfigPair {
    str var_;
    u_short type_;
    enum { STRING = 0,
	   INT    = 1 };
    str vals_;
    int vali_;

    sklist_entry<ConfigPair> hlink_;

    ConfigPair (str var, str val) : var_ (var), type_ (STRING), vals_ (val) {}
    ConfigPair (str var, int val) : var_ (var), type_ (INT), vali_ (val) {}
    void set_str (str val) { type_ = STRING; vals_ = val; }
    void set_int (int val) { type_ = INT; vali_ = val; }
  };

  // NOT IMPLEMENTED
  Configurator (const Configurator &);
  Configurator& operator= (Configurator);
  
  skiplist<ConfigPair, str, &ConfigPair::var_, &ConfigPair::hlink_> conf;
  bool parsed_;
  
 protected:
  Configurator ();
  
 public:
  virtual ~Configurator ();
  static Configurator &only ();

  /** Parse filename to load the configuration information.
      Returns true if filename was parsed correctly.  If
      a configuration file was already loaded, returns false.
      May output errors to log streams. */
  bool parse (const char *filename);
  /** Returns true if a config file has been parsed, false if
      only defaults (or nothing) is available. */
  bool parsed () const;

  void dump ();

  /** Read an int out of Configurator.  Returns true if resp is set. */
  bool get_int (const char *field, int &resp) const;
  /** Read a str out of Configurator.  Returns true if resp is set. */
  bool get_str (const char *field, str &resp) const;

  /** Write newv to field */
  bool set_str (const char *field, str newv);
  /** Write newv to field. */
  bool set_int (const char *field, int newv);
};

#endif // __CONFIGURATOR_H_
