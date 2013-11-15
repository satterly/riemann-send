/*****************************************************************************
 *
 * RIEMANN-SEND-TCP.C
 *
 *    $ indent -br -nut -l125 riemann-send-tcp.c
 *
 *    $ gcc -o riemann-send-tcp riemann-send-tcp.c riemann.pb-c.c $(apr-1-config --cflags --cppflags --includes --link-ld) -lprotobuf-c
 *
 *****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "riemann.pb-c.h"

#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>

#include <apr_general.h>
#include <apr_thread_proc.h>

int done = 0;

char *riemann_server = "localhost";
int riemann_port = 5555;
int riemann_is_available = 0;

int
tokenize (char *str, char *delim, char **tokens)
{
  char *p;
  int i = 0;

  p = strtok (str, delim);
  while (p != NULL) {
    printf ("> %s\n", p);
    tokens[i] = malloc (strlen (p) + 1);
    if (tokens[i])
      strcpy (tokens[i], p);
    i++;
    p = strtok (NULL, delim);
  }
  return i++;
}

static void *APR_THREAD_FUNC
circuit_breaker (apr_thread_t * thd, void *data)
{
  int rc = 1;

  printf ("[cb] start...\n");

  for (; !done;) {
    printf("[cb] checking connection...\n");
    // rc = riemann_connect(riemann_server, riemann_port);
    if (rc == 1) {
      riemann_is_available = 0; /* DOWN */
    }
    else {
      riemann_is_available = 1; /* UP */
      // riemann_close();
    }
    apr_sleep (15*1000*1000);
  }
  apr_thread_exit (thd, APR_SUCCESS);

  return NULL;
}

int
main (int argc, const char *argv[])
{

  apr_status_t rv;
  apr_pool_t *mp;

  apr_initialize ();
  atexit (apr_terminate);

  apr_pool_create (&mp, NULL);

  apr_thread_t *thread;
  if (apr_thread_create (&thread, NULL, circuit_breaker, NULL, mp) != APR_SUCCESS)
    perror ("Failed to create thread. Exiting.\n");

  for (; !done;) {

    Event evt = EVENT__INIT;

    evt.time = 1234567890;
    evt.state = "ok";
    evt.service = "service111";
    evt.host = "myhost";
    evt.description = "this is the description";

    // char *tags[] = { "one", "two", "three", NULL };
    char raw_tags[80] = "cat,foo=bar,yes=yay!";
    // printf ("raw tags = %s\n", raw_tags);

    int n_tags;
    char *tags[1024] = { NULL };

    evt.n_tags = tokenize (raw_tags, ",", tags);
    evt.tags = tags;

    char raw_attrs[80] = "env=PROD,grid=MyGrid,location=paris,foo=bar";

    int n_attrs;
    char *buffer[64] = { NULL };

    n_attrs = tokenize (raw_attrs, ",", buffer);

    Attribute **attrs;
    attrs = malloc (sizeof (Attribute *) * n_attrs);

    int i;
    for (i = 0; i < n_attrs; i++) {

      printf ("buffer[%d] = %s\n", i, buffer[i]);
      char *pair[1] = { NULL };
      tokenize (buffer[i], "=", pair);
      printf ("attributes[%d] -> key = %s value = %s\n", i, pair[0], pair[1]);

      attrs[i] = malloc (sizeof (Attribute));
      attribute__init (attrs[i]);
      attrs[i]->key = pair[0];
      attrs[i]->value = pair[1];
      free (pair[0]);
      free (pair[1]);
      free (buffer[i]);
    }
    evt.attributes = attrs;
    evt.n_attributes = n_attrs;
    printf ("n_attrs = %d\n", n_attrs);

    evt.ttl = 86400;

    evt.has_metric_sint64 = 1;
    evt.metric_sint64 = 123;

    Msg riemann_msg = MSG__INIT;
    void *buf;
    unsigned len;

    riemann_msg.n_events = 1;
    riemann_msg.events = malloc (sizeof (Event) * riemann_msg.n_events);
    riemann_msg.events[0] = &evt;

    len = msg__get_packed_size (&riemann_msg);
    buf = malloc (len);
    msg__pack (&riemann_msg, buf);

    for (i = 0; i < evt.n_tags; i++) {
      printf ("tag %d %s\n", i, tags[i]);
      free (tags[i]);
    }
    fprintf (stderr, "Writing %d serialized bytes\n", len);     // See the length of message

    int sockfd, n;
    struct sockaddr_in servaddr;

    sockfd = socket (AF_INET, SOCK_DGRAM, 0);

    bzero (&servaddr, sizeof (servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr (riemann_server);
    servaddr.sin_port = htons (riemann_port);

    sendto (sockfd, buf, len, 0, (struct sockaddr *) &servaddr, sizeof (servaddr));

    for (i = 0; i < evt.n_attributes; i++) {
      attrs[i]->key = NULL;
      attrs[i]->value = NULL;
      free (attrs[i]);
    }
    free (attrs);
    free (riemann_msg.events);
    free (buf);

    apr_sleep (2*1000*1000);
  }

  return 0;
}
