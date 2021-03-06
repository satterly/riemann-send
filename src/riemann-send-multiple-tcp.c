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

#include <fcntl.h>
#include <errno.h>
#include <sys/poll.h>
#include <apr_general.h>
#include <apr_thread_proc.h>

int done = 0;

#define RIEMANN_CB_OPEN 0
#define RIEMANN_CB_HALF_OPEN 1
#define RIEMANN_CB_CLOSED 2

#define RIEMANN_TIMEOUT 60
#define RIEMANN_MAX_FAILURES 5

char *riemann_server = "127.0.0.1";
char *riemann_protocol = "tcp";
int riemann_port = 5555;
int riemann_circuit_breaker = RIEMANN_CB_CLOSED;
int riemann_reset_timeout = 0;
int riemann_failures = 0;

int riemann_tcp_socket;
struct sockaddr_in servaddr;

int
riemann_connect (const char *server, int port)
{
  printf ("Connecting to %s:%d\n", server, port);

  int sockfd = socket (AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror ("Could not open socket");
    return -1;
  }
  else {
    printf ("Socket created!\n");
  }

  struct sockaddr_in remote_addr = { };
  remote_addr.sin_family = AF_INET;
  remote_addr.sin_port = htons (port);

  if (inet_aton (server, &remote_addr.sin_addr) <= 0) {
    perror ("inet_aton failed");
    return -1;
  }

  if (riemann_tcp_socket)
    close (riemann_tcp_socket);

  // set to non-blocking
  long flags = fcntl (sockfd, F_GETFL, 0);
  fcntl (sockfd, F_SETFL, flags | O_NONBLOCK);

  connect (sockfd, (struct sockaddr *) &remote_addr, sizeof (remote_addr));

  struct pollfd pfd;

  pfd.fd = sockfd;
  pfd.events = POLLOUT;
  int rv = poll (&pfd, 1, 200);

  if (rv < 0) {
    printf ("poll() error\n");
    return -1;
  }
  else if (rv == 0) {
    printf ("timeout\n");
    return -1;
  }

  if (pfd.revents & POLLOUT) {
    printf("socket ready!\n");
    /* fcntl (sockfd, F_SETFL, flags); */
    return sockfd;
  } else {
    return -1;
  }
}

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

Event *
create_riemann_event (const char *grid, const char *cluster, const char *host, const char *ip,
                      const char *metric, const char *value, const char *type, const char *units,
                      const char *state, unsigned int localtime, const char *tags_str,
                      const char *location, unsigned int ttl)
{
  printf
    ("[riemann] grid=%s, cluster=%s, host=%s, ip=%s, metric=%s, value=%s %s, type=%s, state=%s, localtime=%u, tags=%s, location=%s, ttl=%u\n",
     grid, cluster, host, ip, metric, value, units, type, state, localtime, tags_str, location, ttl);

  Event *event = malloc (sizeof (Event));
  event__init (event);

  event->host = (char *) host;
  event->service = (char *) metric;

  if (value) {
    if (!strcmp (type, "int")) {
      event->has_metric_sint64 = 1;
      event->metric_sint64 = strtol (value, (char **) NULL, 10);
    }
    else if (!strcmp (type, "float")) {
      event->has_metric_d = 1;
      event->metric_d = (double) strtod (value, (char **) NULL);
    }
    else {
      event->state = (char *) value;
    }
  }

  event->description = (char *) units;

  if (state)
    event->state = (char *) state;

  if (localtime)
    event->time = localtime;

  char *tags[64] = { NULL };
  char *buffer = strdup (tags_str);

  event->n_tags = tokenize (buffer, ",", tags);
  event->tags = tags;
  free (buffer);

  char attr_str[512];
  sprintf (attr_str, "grid=%s,cluster=%s,ip=%s,location=%s", grid, cluster, ip, location);

  int n_attrs;
  char *kv[64] = { NULL };
  buffer = strdup (attr_str);

  n_attrs = tokenize (buffer, ",", kv);
  free (buffer);

  Attribute **attrs;
  attrs = malloc (sizeof (Attribute *) * n_attrs);

  int i;
  for (i = 0; i < n_attrs; i++) {

    char *pair[1] = { NULL };
    tokenize (kv[i], "=", pair);

    attrs[i] = malloc (sizeof (Attribute));
    attribute__init (attrs[i]);
    attrs[i]->key = pair[0];
    attrs[i]->value = pair[1];
  }
  event->n_attributes = n_attrs;
  event->attributes = attrs;

  event->has_ttl = 1;
  event->ttl = ttl;

  return event;
}

int
send_message_to_riemann (Msg *message)
{
  unsigned len;
  int nbytes = 0;

  if (riemann_circuit_breaker == RIEMANN_CB_CLOSED) {
    if (!strcmp (riemann_protocol, "udp")) {

      void *buf;
      len = msg__get_packed_size (message);
      buf = malloc (len);
      msg__pack (message, buf);

      // nbytes = sendto (riemann_tcp_socket, buf, len, 0, (struct sockaddr *) &servaddr, sizeof (servaddr));
      free (buf);
    }
    else {
      printf ("[riemann] Sending metric via TCP...");
      struct
      {
        uint32_t header;
        uint8_t data[0];
      } *buf;

      len = msg__get_packed_size (message) + sizeof (buf->header);
      buf = malloc (len);
      msg__pack (message, buf->data);
      buf->header = htonl (len - sizeof (buf->header));

      nbytes = send (riemann_tcp_socket, buf, len, 0);
      free (buf);

      Msg *response;
      uint32_t header, len;
      uint8_t *buffer;
      ssize_t rbytes;

      printf ("wait for response...\n");
      rbytes = recv (riemann_tcp_socket, &header, sizeof (header), 0);
      if (rbytes != sizeof (header)) {
        printf ("[riemann] error in response\n");
      }
      else {
        len = ntohl (header);
        printf ("header is %d\n", len);
        buffer = malloc (len);
        rbytes = recv (riemann_tcp_socket, buffer, len, 0);
        response = msg__unpack (NULL, len, buffer);
        printf ("ok %d\n", response->ok);
        if (!response->ok)  printf("NOT OK\n");
        free (buffer);
      }
    }

    if (nbytes != len) {
      fprintf (stderr, "[riemann] sendto socket (client): %s\n", strerror (errno));
      riemann_failures++;
      if (riemann_failures > RIEMANN_MAX_FAILURES) {
        riemann_circuit_breaker = RIEMANN_CB_OPEN;
        riemann_reset_timeout = apr_time_now () + RIEMANN_TIMEOUT;
      }
      return EXIT_FAILURE;
    }
    else {
      riemann_failures = 0;
      printf ("[riemann] Sent %d serialized bytes\n", len);
    }
  }
  else if (riemann_circuit_breaker == RIEMANN_CB_OPEN) {
    printf ("[riemann] Circuit breaker OPEN... Not sending metric via TCP! Riemann DOWN!!!\n");
  }
}

int
delete_riemann_event(Event *event)
{
  int i;
/*
  for (i = 0; i < event->n_attributes; i++) {
    free (attrs[i]->key);
    free (attrs[i]->value);
    free (attrs[i]);
    free (kv[i]);
  }
  free (attrs);

  for (i = 0; i < event->n_tags; i++) {
    free (tags[i]);
  }
*/
}


static void *APR_THREAD_FUNC
circuit_breaker (apr_thread_t * thd, void *data)
{
  int rc = 0;

  printf ("[cb] start...\n");

  for (; !done;) {

    if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {
      printf ("Reset period expired, retry connection...\n");
      riemann_circuit_breaker = RIEMANN_CB_HALF_OPEN;
      /* retry connection */
      riemann_tcp_socket = riemann_connect (riemann_server, riemann_port);
      printf("riemann_tcp_socket = %d\n", riemann_tcp_socket);
      if (riemann_tcp_socket < 0) {
        riemann_circuit_breaker = RIEMANN_CB_OPEN;
        riemann_reset_timeout = apr_time_now () + RIEMANN_TIMEOUT;      /* 60 seconds */
      }
      else {
        riemann_failures = 0;
        riemann_circuit_breaker = RIEMANN_CB_CLOSED;
      }
    }

    printf ("[cb] riemann is %s\n",
            riemann_circuit_breaker == RIEMANN_CB_OPEN ? "OPEN" :
            riemann_circuit_breaker == RIEMANN_CB_HALF_OPEN ? "HALF_OPEN"
            /* RIEMANN_CB_CLOSED */ : "CLOSED");

  }

  apr_thread_exit (thd, APR_SUCCESS);

  return NULL;
}

int
main (int argc, const char *argv[])
{
  signal (SIGPIPE, SIG_IGN);

  apr_status_t rv;
  apr_pool_t *mp;

  apr_initialize ();
  atexit (apr_terminate);

  apr_pool_create (&mp, NULL);

  apr_thread_t *thread;
  if (apr_thread_create (&thread, NULL, circuit_breaker, NULL, mp) != APR_SUCCESS)
    perror ("Failed to create thread. Exiting.\n");

  if (!strcmp (riemann_protocol, "udp")) {

    printf ("[riemann] set up UDP connection...\n");

    riemann_tcp_socket = socket (AF_INET, SOCK_DGRAM, 0);

    bzero (&servaddr, sizeof (servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr ("127.0.0.1");
    servaddr.sin_port = htons (5555);

    riemann_circuit_breaker = RIEMANN_CB_CLOSED;
  }
  else {
    printf ("[riemann] set up TCP connection...\n");
    riemann_tcp_socket = riemann_connect (riemann_server, riemann_port);
    if (riemann_tcp_socket < 0) {
      printf ("[riemann] circuit breaker OPEN...\n");
      riemann_circuit_breaker = RIEMANN_CB_OPEN;
    }
    else {
      printf ("[riemann] circuit breaker CLOSED...\n");
      riemann_circuit_breaker = RIEMANN_CB_CLOSED;
    }
  }

  /* main */

  Event *event;
  event = create_riemann_event ("MyGrid", "clust01", "myhost555", "10.1.1.1", "cpu_system", "100.0", "float", "%", "ok",
                          1234567890, "tag1,tag2", "london", 180);

  printf ("[riemann] %zu host=%s, service=%s, state=%s, metric_f=%f, metric_d=%lf, metric_sint64=%" PRId64
          ", description=%s, ttl=%f, tags(%zu), attributes(%zu)\n", event->time, event->host, event->service, event->state, event->metric_f,
          event->metric_d, event->metric_sint64, event->description, event->ttl, event->n_tags, event->n_attributes);

  Event *event2;
  event2 = create_riemann_event ("MyGrid2", "clust02", "myhost222", "20.2.2.2", "cpu_system", "100.0", "float", "%", "ok",
                          1234567890, "tag1,tag2", "london", 180);

  Msg *riemann_msg;
  riemann_msg = malloc (sizeof (Msg));
  msg__init (riemann_msg);

  riemann_msg->events = malloc (sizeof (Event) * 2);  /* FIXME realloc() */
  riemann_msg->n_events = 2;
  riemann_msg->events[0] = event;
  riemann_msg->events[1] = event2;

  delete_riemann_event(event);
  delete_riemann_event(event2);

  printf("sending...\n");
  send_message_to_riemann(riemann_msg);
  printf("sent!\n");

  free (riemann_msg->events);
}
