/* $Id: http.C,v 1.4 2003/01/02 22:04:28 jastr Exp $ */

/*
 *
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

/*cause strptime to be defined on planetlab*/
#if defined(__linux) and !defined(_GNU_SOURCE) 
#  define _GNU_SOURCE
#endif

#include "http.h"
#include "rxx.h"
#include "sha1.h"
#include <pwd.h>
#include <time.h>

static rxx hdrnameval ("^([^\\s:]+):\\s*(.*)\r?\n\\z", "s");

str
suio_remstr (suio *uio, size_t n)
{
  mstr m (n);
  uio->copyout (m, n);
  uio->rembytes (n);
  return m;
}

str
hashstr (str src)
{
  char hash[sha1::hashsize];
  sha1_hash (hash, src, src.len ());
  return armor32 (hash, sizeof (hash));
}

str
httptime ()
{
  char buf[30];
  time_t now = time (NULL);
  int n = strftime (buf, 30, "%a, %d %b %Y %H:%M:%S GMT", gmtime (&now));
  assert (n == 29);
  return buf;
}

str
httperror (int status, str statmsg, str url, str description)
{
  static str server;
  if (!server) {
    struct passwd *pw = NULL;
    if (char *login = getlogin ())
      pw = getpwnam (login);
    if (!pw)
      pw = getpwuid (getuid ());
    if (pw && *pw->pw_gecos)
      server = strbuf ("%s's proxy server", pw->pw_gecos);
    else
      server = "G22.3033-010 proxy server";
  }

  strbuf body;
  body << "<HTML><HEAD>\n\
<TITLE>ERROR: The requested URL could not be retrieved</TITLE>\n\
</HEAD><BODY>\n\
<H1>ERROR</H1>\n\
<H2>The requested URL could not be retrieved</H2>\n\
<HR>\n\
<P>\n\
While trying to retrieve the URL:\n"
       << "<A HREF=\"" << url << "\">" << url << "</A>\n\
<P>\n\
The following error was encountered:\n\
<P>\n"
       << "<STRONG>" << description << "</STRONG><BR>\n"
       << "<P>" << server << " at " << myname () << "\n"
       << "</BODY></HTML>\n";

  strbuf head ("HTTP/1.0 %03d ", status);
  head << statmsg << "\r\n";
  str now = httptime ();
  head << "Mime-Version: 1.0\r\nContent-Type: text/html\r\n"
       << "Content-Length: " << body.tosuio ()->resid () << "\r\n"
       << "Server: " << server << "\r\n"
       << "Date: " << now << "\r\n"
       << "Expires: " << now << "\r\n\r\n";
  head.tosuio ()->take (body.tosuio ());
  return head;
}

str
httpparse::gethdr (suio *uio)
{
  size_t n = 0;
  for (const iovec *v = uio->iov (), *e = uio->iovlim ();
       v < e; n += v++->iov_len) {
    const char *base = static_cast<char *> (v->iov_base);
    const char *lim = base + v->iov_len;
    const char *p = base;
    while ((p = static_cast<char *> (memchr (p, '\n', v->iov_len)))) {
      p++;
      if (n + (p - base) == 1) {
	uio->rembytes (1);
	return "";
      }
      if (n + (p - base) == 2
	  && *static_cast<char *> (uio->iov ()->iov_base) == '\r') {
	uio->rembytes (2);
	return "";
      }
      if (p < lim) {
	if (*p == ' ' || *p == '\t')
	  continue;
      }
      else if (v + 1 < e) {
	if (*static_cast<char *> (v[1].iov_base) == ' '
	    || *static_cast<char *> (v[1].iov_base) == '\t')
	  continue;
      }
      else
	/* Might or might not have complete line, need next char to tell. */
	return NULL;
      return suio_remstr (uio, n + (p - base));
    }
  }
  return NULL;
}

str
httpparse::hdrname (str hdr)
{
  if (!hdrnameval.search (hdr))
    return NULL;
  return str2lower (hdrnameval[1]);
}

str
httpparse::hdrval (str hdr)
{
  if (!hdrnameval.search (hdr))
    panic << "hdrval: bad header: " << hdr << "\n";
  return hdrnameval[2];
}

time_t
httpparse::parsetime (const char *text)
{
  time_t t;
  struct tm stm;
  bzero (&stm, sizeof (stm));
  if (!strptime (text, "%a, %d %b %Y %H:%M:%S GMT", &stm)
      && !strptime (text, "%a %b %d %H:%M:%S %Y", &stm))
    return -1;
  t = mktime (&stm);
  t += stm.tm_gmtoff;
  return t;
}

bool
httpparse::checkpragma (str val, str pragma)
{
  strbuf rxb;
  rxb << "(^|,)\\s*";
  for (const char *p = pragma, *e = p + pragma.len (); p < e; p++)
    if (isalnum (*p))
      rxb.tosuio ()->print (p, 1);
    else {
      rxb.tosuio ()->print ("\\", 1);
      rxb.tosuio ()->print (p, 1);
    }
  rxb << "\\s*(,|$)";
  str s (rxb);
  rxx pragmarx (s, "i");
  return pragmarx.search (val);
}

static rxx urlrx ("^http://([\\w.-]+)(:(\\d+))?(/\\S*)?", "i");
bool
httpparse::parseurl (str *host, u_int16_t *port, str *path, str url)
{
  if (!urlrx.search (url))
    return false;
  *host = str2lower (urlrx[1]);
  *port = 80;
  if (str p = urlrx[3])
    convertint (p, port);

  if (!(*path = urlrx[4]))
    *path = "/";
  return true;
}

extern char hostname[1024];
extern int hostport;

static rxx reqrx ("^(\\S+)\\s+(/\\S*)", "i");
bool
httpparse::parsereq (str *method, str *url, str line)
{
  if (!reqrx.search (line))
    return false;
  *method = str2upper (reqrx[1]);

  if (hostport == 80)
    *url = strbuf () << "http://" << hostname << reqrx[2];
  else
    *url = strbuf () << "http://" << hostname << ":" << hostport << reqrx[2];

  return true;
}

static rxx resprx ("^HTTP/\\d+\\.\\d+\\s+([1-5]\\d\\d)\\s+(.*)", "i");
bool
httpparse::parseresp (int *status, str *msg, str line)
{
  if (!resprx.search (line))
    return false;
  convertint (resprx[1], status);
  *msg = resprx[2];
  return true;
}

int
httpparse::parse (suio *uio)
{
  while (str h = gethdr (uio)) {
    if (!firstdone) {
      if (!dofirst (h))
	return -1;
      firstdone = true;
      continue;
    }
    if (!h.len ())
      return 1;
    if (!hdrnameval.search (h))
      return -1;
    if (!doheader (str2lower (hdrnameval[1]), hdrnameval[2]))
      headers << h;
  }
  return 0;
}

bool
httpreq::dofirst (str line)
{
  if (!parsereq (&method, &url, line)
      || !parseurl (&host, &port, &path, url))
    return false;
  headers << method << " " << path << " HTTP/1.0\r\n";
  urlhash = hashstr (url);
  return true;
}

bool
httpreq::doheader (str name, str val)
{
  if (name == "pragma") {
    if (checkpragma (val, "no-cache"))
      nocache = true;
  }
  else if (name == "if-modified-since") {
    if_modified_since = parsetime (val);
  }
  else if (name == "content-length")
    content_length = atoi (val.cstr ());
  //    convertint (val, &content_length);
  else if (name == "authorization")
    authorization = val;
  else if (name == "referer")
    parseurl(&referer_host, &r_port, &r_path, val);
  return false;
}

bool
httpresp::dofirst (str line)
{
  if (!parseresp (&status, &statusmsg, line))
    return false;
  headers << line;
  return true;
}

bool
httpresp::doheader (str name, str val)
{
  if (name == "pragma") {
    if (checkpragma (val, "no-cache"))
      nocache = true;
  }
  else if (name == "date")
    date = parsetime (val);
  else if (name == "expires")
    expires = parsetime (val);
  else if (name == "last-modified")
    last_modified = parsetime (val);
  else if (name == "content-length")
    content_length = atoi (val.cstr ());
  return false;
}
