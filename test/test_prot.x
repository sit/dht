/*
 * Ticker-tape serializer RPC definitions
 *
 * rpcc -c ticker.x ; rpcc -h ticker.x
 */

struct test_result {
  int ok;
};

program DHASH_TEST_PROG {
  version DHASH_TEST_VERS {
    test_result TEST_BLOCK (void) = 1;
    test_result TEST_UNBLOCK (void) = 2;
  } = 1;
} = 400001;
