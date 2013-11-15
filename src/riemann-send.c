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
    tokens[i] = malloc (strlen (p) + 1);
    if (tokens[i])
      strcpy (tokens[i], p);
    i++;
    p = strtok (NULL, delim);
  }
  return i++;
}

int
send_data_to_riemann (const char *grid, const char *cluster, const char *host, const char *ip,
                      const char *metric, const char *value, const char *type, const char *units,
                      const char *state, unsigned int localtime, const char *tags_str,
                      const char *location, unsigned int ttl)
{

  int i;
  char *buffer = NULL;

  printf("[riemann] grid=%s, cluster=%s, host=%s, ip=%s, metric=%s, value=%s %s, type=%s, state=%s, localtime=%u, tags=%s, location=%s, ttl=%u\n",
            grid, cluster, host, ip, metric, value, units, type, state, localtime, tags_str, location, ttl);

  Event evt = EVENT__INIT;

  evt.host = (char *)host;
  evt.service = (char *)metric;

   if (value) {
       if (!strcmp(type, "int")) {
           evt.has_metric_sint64 = 1;
           evt.metric_sint64 = strtol(value, (char **) NULL , 10 );
       } else if (!strcmp(type, "float")) {
           evt.has_metric_d = 1;
           evt.metric_d = (double) strtod(value, (char**) NULL);
       } else {
           evt.state = (char *)value;
       }
   }
  evt.description = (char *)units;

   if (state)
      evt.state = (char *)state;

   if (localtime)
      evt.time = localtime;

  char *tags[64] = { NULL };
  buffer = strdup(tags_str);

  evt.n_tags = tokenize (buffer, ",", tags);  /* assume tags are comma-separated */
  evt.tags = tags;
  free(buffer);

  char attr_str[512];
  sprintf(attr_str, "grid=%s,cluster=%s,ip=%s,location=%s", grid, cluster, ip, location);

  int n_attrs;
  char *kv[64] = { NULL };
  buffer = strdup(attr_str);

  n_attrs = tokenize (buffer, ",", kv);
  free(buffer);

  Attribute **attrs;
  attrs = malloc (sizeof (Attribute *) * n_attrs);

  for (i = 0; i < n_attrs; i++) {

    char *pair[1] = { NULL };
    tokenize (kv[i], "=", pair);

    attrs[i] = malloc (sizeof (Attribute));
    attribute__init (attrs[i]);
    attrs[i]->key = pair[0];
    attrs[i]->value = pair[1];
  }
  evt.n_attributes = n_attrs;
  evt.attributes = attrs;

  evt.has_ttl = 1;
  evt.ttl = ttl;

  Msg riemann_msg = MSG__INIT;
  void *buf;
  unsigned len;

  riemann_msg.n_events = 1;
  riemann_msg.events = malloc(sizeof (Event) * riemann_msg.n_events);
  riemann_msg.events[0] = &evt;

  len = msg__get_packed_size(&riemann_msg);
  buf = malloc(len);
  msg__pack(&riemann_msg, buf);

  printf("[riemann] %zu host=%s, service=%s, state=%s, metric_f=%f, metric_d=%lf, metric_sint64=%" PRId64 ", description=%s, ttl=%f, tags(%zu), attributes(%zu)\n", evt.time, evt.host, evt.service, evt.state, evt.metric_f, evt.metric_d, evt.metric_sint64, evt.description, evt.ttl, evt.n_tags, evt.n_attributes);


  int sockfd, nbytes;
  struct sockaddr_in servaddr;
  sockfd = socket (AF_INET, SOCK_DGRAM, 0);

  bzero (&servaddr, sizeof (servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = inet_addr ("127.0.0.1");
  servaddr.sin_port = htons (5555);

  nbytes = sendto (sockfd, buf, len, 0, (struct sockaddr *) &servaddr, sizeof (servaddr));

  if (nbytes != len)
  {
         fprintf(stderr, "[riemann] sendto socket (client): %s\n", strerror(errno));
         return EXIT_FAILURE;
  } else {
      printf("[riemann] Sent %d serialized bytes\n", len);
  }

  for (i = 0; i < evt.n_attributes; i++) {
     free(attrs[i]->key);
     free(attrs[i]->value);
     free(attrs[i]);
     free(kv[i]);
  }
  free(attrs);
  for (i = 0; i < evt.n_tags; i++) {
     free(tags[i]);
  }
  free(riemann_msg.events);
  free(buf);

  return 0;

}


int
main (int argc, const char *argv[])
{

  send_data_to_riemann ("MyGrid", "clust01", "myhost555", "10.1.1.1", "cpu_system", "100.0", "float", "%", "ok", 1234567890, "tag1,tag2", "london", 180);

}
