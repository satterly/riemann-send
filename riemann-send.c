/*****************************************************************************
 *
 * RIEMANN-SEND.C
 *
 *    $ indent -br -nut -l125 riemann-send.c
 *
 *    $ gcc -o riemann-send riemann-send.c riemann.pb-c.c -lprotobuf-c
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

int
main (int argc, const char *argv[])
{

  Event evt = EVENT__INIT;

  evt.time = 1234567890;
  evt.state = "ok";
  evt.service = "service111";
  evt.host = "myhost";
  evt.description = "this is the description";

  // char *tags[] = { "one", "two", "three", NULL };
  char raw_tags[80] = "cat=dog,length=1,wibble";
  // printf ("raw tags = %s\n", raw_tags);

  int n_tags;
  char *tags[64] = { NULL };

  evt.n_tags = tokenize (raw_tags, ",", tags);
  evt.tags = tags;

  char raw_attrs[80] = "environment=PROD,grid=MyGrid,location=paris";

  int n_attrs;
  char *buffer[64] = { NULL };

  n_attrs = tokenize (raw_attrs, ",", buffer);


  Attribute **attrs;
  attrs = malloc (sizeof (Attribute *) * n_attrs);

  int i;
  for (i = 0; i < n_attrs; i++) {

    printf ("buffer[%d] = %s\n", i, buffer[i]);
    char *a[32] = { NULL };
    tokenize (buffer[i], "=", a);
    printf ("attributes[%d] -> a[0] = %s a[1] = %s\n", i, a[0], a[1]);

    attrs[i] = malloc (sizeof (Attribute));
    attribute__init (attrs[i]);
    attrs[i]->key = a[0];
    attrs[i]->value = a[1];
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
  riemann_msg.events = malloc(sizeof (Event) * riemann_msg.n_events);
  riemann_msg.events[0] = &evt;

  len = msg__get_packed_size(&riemann_msg);
  buf = malloc(len);
  msg__pack(&riemann_msg, buf);

  fprintf (stderr, "Writing %d serialized bytes\n", len);       // See the length of message

  int sockfd, n;
  struct sockaddr_in servaddr;

  sockfd = socket (AF_INET, SOCK_DGRAM, 0);

  bzero (&servaddr, sizeof (servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = inet_addr ("127.0.0.1");
  servaddr.sin_port = htons (5555);

  sendto (sockfd, buf, strlen (buf), 0, (struct sockaddr *) &servaddr, sizeof (servaddr));

  free (evt.attributes);        // Free the allocated serialized buffer
  free (evt.tags);              // Free the allocated serialized buffer
  free (buf);               // Free the allocated serialized buffer

  return 0;
}
