#include "async.h"
#include "esign.h"
#include "xmms/xmmsctrl.h"
#include "ihash.h"
#include "sys/time.h"

struct msg {
  char user;
  int numsongs;
  char songs[];
  int sequence;
  bigint sig;
};

timespec record_beyond, send_beyond;
int sessionid = 0;
// static?
// FIXME remove rxx stuff?
//rxx pathrxx("([^/]*)$");

struct song_record {
  str name;
  unsigned int second_count;
  ihash_entry <song_record> link;

  song_record(str n) : name(n), second_count(0) {};
  void inc() { second_count++; };
};

ihash <str, song_record, &song_record::name, &song_record::link> songs;

void
record(void) {
  const char *s = "";

  record_beyond.tv_sec++;
  timecb(record_beyond, wrap(&record));

  if(xmms_remote_is_running(sessionid) &&
//     xmms_remote_is_playing(sessionid) &&
//     !xmms_remote_is_paused(sessionid) &&
     (s = xmms_remote_get_playlist_file(sessionid,
					xmms_remote_get_playlist_pos(sessionid)))) {
    song_record *sr = songs[s];
    if(!sr) {
      sr = New song_record(s);
      songs.insert(sr);
    }
    sr->inc();

    //    if(pathrxx.search(s))
    //      s = pathrxx[1];
  }

  timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  if(!s) s = "";
  warn << "hiya " << now.tv_sec << "." << now.tv_nsec << " " << s << "\n";
}

void
send(void) {
  send_beyond.tv_sec += 60;
  timecb(send_beyond, wrap(&send));

  timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  for(song_record *sr = songs.first(); sr; sr = songs.next(sr))
    warn << "bye " << now.tv_sec << "." << now.tv_nsec << " " << sr->name << ", " << sr->second_count << "\n";
  songs.deleteall();
}

int
main(void) {
  timespec now;

  clock_gettime(CLOCK_REALTIME, &now);
  record_beyond = now;
  record_beyond.tv_sec++;
  timecb(record_beyond, wrap(&record));
  record_beyond.tv_sec++;
  timecb(record_beyond, wrap(&record));

  clock_gettime(CLOCK_REALTIME, &now);
  send_beyond = now;
  send_beyond.tv_sec += 60;
  timecb(send_beyond, wrap(&send));
  send_beyond.tv_sec += 60;
  timecb(send_beyond, wrap(&send));

  str msg = "hello hello";
  esign_priv priv = esign_keygen(1024);
  bigint sig = priv.sign(msg);
  warn << msg << ": " << sig << "\n";

  if(priv.verify(msg, sig))
    warn << "test ok!\n";
  else
    warn << "verify failure\n";

  str badmsg = "hello helln";
  if(priv.verify(badmsg, sig))
    warn << "verify failure\n";
  else
    warn << "test ok!\n";

  amain();
  return 0;
}
