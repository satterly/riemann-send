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

#define err_quit printf
#define err_msg printf
#define debug_msg printf

int done = 0;

#define RIEMANN_CB_OPEN 0
#define RIEMANN_CB_HALF_OPEN 1
#define RIEMANN_CB_CLOSED 2

#define RIEMANN_RETRY_TIMEOUT 60
#define RIEMANN_MAX_FAILURES 5
#define RIEMANN_TCP_TIMEOUT 500 /* ms */

pthread_mutex_t  riemann_mutex;

char *riemann_server = "127.0.0.1";
char *riemann_protocol = "tcp";
int riemann_port = 5555;
int riemann_circuit_breaker = RIEMANN_CB_CLOSED;
int riemann_reset_timeout = 0;
int riemann_failures = 0;

char *riemann_attributes = "environment=PROD,customer=Nokia";

__thread Msg *riemann_msg = NULL;
__thread int riemann_num_events;

typedef struct
{
  int sockfd;
  struct sockaddr sa;
  unsigned int ref_count;
} g_mcast_socket;
typedef g_mcast_socket g_tcp_socket;
typedef g_mcast_socket g_udp_socket;

g_udp_socket *riemann_udp_socket;
g_tcp_socket *riemann_tcp_socket;

g_udp_socket*
init_riemann_udp_socket (const char *hostname, uint16_t port)
{
   int sockfd;
   g_udp_socket* s;
   struct sockaddr_in *sa_in;
   struct hostent *hostinfo;

   sockfd = socket(AF_INET, SOCK_DGRAM, 0);
   if (sockfd < 0)
      return NULL;

   s = malloc( sizeof( g_udp_socket ) );
   memset( s, 0, sizeof( g_udp_socket ));
   s->sockfd = sockfd;
   s->ref_count = 1;

   /* Set up address and port for connection */
   sa_in = (struct sockaddr_in*) &s->sa;
   sa_in->sin_family = AF_INET;
   sa_in->sin_port = htons (port);
   hostinfo = gethostbyname (hostname);
   if (!hostinfo)
      err_quit("Unknown host %s", hostname);
   sa_in->sin_addr = *(struct in_addr *) hostinfo->h_addr;

   return s;
}

g_tcp_socket*
init_riemann_tcp_socket (const char *hostname, uint16_t port)
{
  int sockfd;
  g_tcp_socket* s;
  struct sockaddr_in *sa_in;
  struct hostent *hostinfo;
  int rv;

  sockfd = socket (AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
      return NULL;

  s = malloc( sizeof( g_tcp_socket ) );
  memset( s, 0, sizeof( g_tcp_socket ));
  s->sockfd = sockfd;
  s->ref_count = 1;

  /* Set up address and port for connection */
  sa_in = (struct sockaddr_in*) &s->sa;
  sa_in->sin_family = AF_INET;
  sa_in->sin_port = htons (port);
  hostinfo = gethostbyname (hostname);
  if (!hostinfo)
      err_quit("Unknown host %s", hostname);
  sa_in->sin_addr = *(struct in_addr *) hostinfo->h_addr;

  if (riemann_tcp_socket)
      close (riemann_tcp_socket->sockfd);

  struct timeval timeout;
  timeout.tv_sec = 0;
  timeout.tv_usec = RIEMANN_TCP_TIMEOUT * 1000;

  if (setsockopt (sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(timeout)) < 0)
    {
      close (sockfd);
      free (s);
      return NULL;
    }

  if (setsockopt (sockfd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout)) < 0)
    {
      close (sockfd);
      free (s);
      return NULL;
    }

  rv = connect(sockfd, &s->sa, sizeof(s->sa));
  if (rv != 0)
    {
      close (sockfd);
      free (s);
      return NULL;
    }

  return s;
}

int
tokenize (const char *str, char *delim, char **tokens)
{
  debug_msg("[token] start\n");

  char *copy = strdup (str);
  char *p;
  char *last;
  int i = 0;

  p = strtok_r (copy, delim, &last);
  while (p != NULL) {
    tokens[i] = malloc (strlen (p) + 1);
    if (tokens[i])
      strcpy (tokens[i], p);
    i++;
    p = strtok_r (NULL, delim, &last);
  }
  debug_msg("[token] end\n");
  free (copy);
  return i++;
}

Event *
create_riemann_event (const char *grid, const char *cluster, const char *host, const char *ip,
                      const char *metric, const char *value, const char *type, const char *units,
                      const char *state, unsigned int localtime, const char *tags_str,
                      const char *location, unsigned int ttl)
{
  debug_msg("[riemann] grid=%s, cluster=%s, host=%s, ip=%s, metric=%s, value=%s %s, type=%s, state=%s, "
            "localtime=%u, tags=%s, location=%s, ttl=%u\n", grid, cluster, host, ip, metric, value,
            units, type, state, localtime, tags_str, location, ttl);

  Event *event = malloc (sizeof (Event));
  event__init (event);

  event->host = strdup (host);
  event->service = strdup (metric);

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
      event->state = strdup (value);
    }
  }

  event->description = strdup (units);

  if (state)
    event->state = strdup (state);

  if (localtime)
    event->time = localtime;

  char *tags[64] = { NULL };

  event->n_tags = tokenize (tags_str, ",", tags);
  event->tags = malloc (sizeof (char *) * (event->n_tags));
  int j;
  for (j = 0; j< event->n_tags; j++) {
     event->tags[j] = strdup (tags[j]);
     debug_msg("[riemann] tag[%d] = %s\n", j, event->tags[j]);
     free(tags[j]);
  }

  char attr_str[512];
  sprintf(attr_str, "grid=%s,cluster=%s,ip=%s,location=%s%s%s", grid, cluster, ip, location,
        riemann_attributes ? "," : "",
        riemann_attributes ? riemann_attributes : "");

  char *kv[64] = { NULL };
  event->n_attributes = tokenize (attr_str, ",", kv);

  Attribute **attrs;
  attrs = malloc (sizeof (Attribute *) * (event->n_attributes ));

  int i;
  for (i = 0; i < event->n_attributes; i++) {

    char *pair[2] = { NULL };
    tokenize (kv[i], "=", pair);
    free(kv[i]);

    attrs[i] = malloc (sizeof (Attribute));
    attribute__init (attrs[i]);
    attrs[i]->key = strdup(pair[0]);
    attrs[i]->value = strdup(pair[1]);
    free(pair[0]);
    free(pair[1]);
  }
  event->attributes = attrs;

  event->has_ttl = 1;
  event->ttl = ttl;

  debug_msg("[riemann] %zu host=%s, service=%s, state=%s, metric_f=%f, metric_d=%lf, metric_sint64=%" PRId64
            ", description=%s, ttl=%f, tags(%zu), attributes(%zu)\n", event->time, event->host, event->service,
            event->state, event->metric_f, event->metric_d, event->metric_sint64, event->description,
            event->ttl, event->n_tags, event->n_attributes);

  return event;
}

int
send_event_to_riemann (Event *event)
{
   int nbytes;
   unsigned len;
   void *buf;

   Msg riemann_msg = MSG__INIT;
   riemann_msg.n_events = 1;
   riemann_msg.events = malloc(sizeof (Event));
   riemann_msg.events[0] = event;

   len = msg__get_packed_size(&riemann_msg);
   buf = malloc(len);
   msg__pack(&riemann_msg, buf);

   pthread_mutex_lock( &riemann_mutex );
   nbytes = sendto (riemann_udp_socket->sockfd, buf, len, 0,
                      (struct sockaddr_in*)&riemann_udp_socket->sa, sizeof (struct sockaddr_in));
   pthread_mutex_unlock( &riemann_mutex );
   free (buf);

   if (nbytes != len) {
      err_msg ("[riemann] ERROR UDP socket sendto(): %s\n", strerror (errno));
      return EXIT_FAILURE;
   } else {
      debug_msg ("[riemann] Sent %d serialized bytes\n", len);
   }
   return EXIT_SUCCESS;
}

int
send_message_to_riemann (Msg *message)
{
   int rval = EXIT_SUCCESS;

   if (riemann_circuit_breaker == RIEMANN_CB_CLOSED) {

      uint32_t len;
      ssize_t nbytes;
      struct {
         uint32_t header;
         uint8_t data[0];
      } *sbuf;

      if (!message)
         return EXIT_FAILURE;

      len = msg__get_packed_size (message) + sizeof (sbuf->header);
      sbuf = malloc (len);
      msg__pack (message, sbuf->data);
      sbuf->header = htonl (len - sizeof (sbuf->header));

      pthread_mutex_lock( &riemann_mutex );
      nbytes = send (riemann_tcp_socket->sockfd, sbuf, len, 0);
      free (sbuf);

      if (nbytes != len) {
         err_msg("[riemann] ERROR TCP socket send(): %s\n", strerror (errno));
         riemann_failures++;
         rval = EXIT_FAILURE;
      } else {
         debug_msg("[riemann] Sent %d serialized bytes\n", len);
      }

      Msg *response;
      uint32_t header;
      uint8_t *rbuf;

      nbytes = recv (riemann_tcp_socket->sockfd, &header, sizeof (header), 0);
      pthread_mutex_unlock( &riemann_mutex );

      if (nbytes == 0) {  /* server closed connection */
         err_msg ("[riemann] server closed connection");
         riemann_failures = RIEMANN_MAX_FAILURES + 1;
         rval = EXIT_FAILURE;
      } else if (nbytes == -1) {
         err_msg ("[riemann] %s", strerror(errno));
         riemann_failures++;
         rval = EXIT_FAILURE;
      } else if (nbytes != sizeof (header)) {
         err_msg ("[riemann] error occurred receiving response");
         riemann_failures++;
         rval = EXIT_FAILURE;
      } else {
         len = ntohl (header);
         rbuf = malloc (len);
         pthread_mutex_lock( &riemann_mutex );
         recv (riemann_tcp_socket->sockfd, rbuf, len, 0);
         pthread_mutex_unlock( &riemann_mutex );
         response = msg__unpack (NULL, len, rbuf);
         debug_msg ("[riemann] message response ok=%d\n", response->ok);
         free (rbuf);

         if (response->ok != 1) {
            err_msg("[riemann] message response error: %s", response->error);
            riemann_failures++;
            rval = EXIT_FAILURE;
         }
      }
      if (riemann_failures > RIEMANN_MAX_FAILURES) {
       riemann_circuit_breaker = RIEMANN_CB_OPEN;
       riemann_reset_timeout = apr_time_now () + RIEMANN_RETRY_TIMEOUT * APR_USEC_PER_SEC;
       err_msg("[riemann] %d send failures exceeds maximum of %d - circuit breaker is OPEN for %d seconds\n",
          riemann_failures, RIEMANN_MAX_FAILURES, RIEMANN_RETRY_TIMEOUT);
      }
   }
  return rval;
}

/* Interval (seconds) between runs */
#define CIRCUIT_BREAKER_INTERVAL 10

static void *APR_THREAD_FUNC
circuit_breaker (apr_thread_t * thd, void *data)
{
   for (;;) {

      if (riemann_circuit_breaker == RIEMANN_CB_OPEN && riemann_reset_timeout < apr_time_now ()) {

         pthread_mutex_lock( &riemann_mutex );

         debug_msg ("[riemann] Reset period expired, retry connection...");
         riemann_circuit_breaker = RIEMANN_CB_HALF_OPEN;

         riemann_tcp_socket = init_riemann_tcp_socket (riemann_server, riemann_port);

         if (riemann_tcp_socket == NULL) {
            riemann_circuit_breaker = RIEMANN_CB_OPEN;
            riemann_reset_timeout = apr_time_now () + RIEMANN_RETRY_TIMEOUT * APR_USEC_PER_SEC;
         } else {
            riemann_circuit_breaker = RIEMANN_CB_CLOSED;
            riemann_failures = 0;
         }
         pthread_mutex_unlock( &riemann_mutex );
      }

      debug_msg("[riemann] circuit breaker is %s (%d)",
            riemann_circuit_breaker == RIEMANN_CB_CLOSED ? "CLOSED" :
            riemann_circuit_breaker == RIEMANN_CB_OPEN ?   "OPEN"
                              /* RIEMANN_CB_HALF_OPEN */ : "HALF_OPEN",
            riemann_circuit_breaker);

      apr_sleep(apr_time_from_sec(CIRCUIT_BREAKER_INTERVAL));
   }
}

int
destroy_riemann_event(Event *event)
{
   int i;
   if (event->host)
      free(event->host);
   if (event->service)
      free(event->service);
   if (event->state)
      free(event->state);
   if (event->description)
      free(event->description);
   printf("destroying tags...\n");
   for (i = 0; i < event->n_tags; i++)
      free(event->tags[i]);
   if (event->tags)
      free(event->tags);
   printf("destroying attributes...\n");
   for (i = 0; i < event->n_attributes; i++) {
      free(event->attributes[i]->key);
      free(event->attributes[i]->value);
      free(event->attributes[i]);
   }
   if (event->attributes)
      free(event->attributes);
   printf("destroying event...\n");
   free (event);
}

int
destroy_riemann_msg(Msg *message)
{
   int i;
   for (i = 0; i < message->n_events; i++) {
     destroy_riemann_event(message->events[i]);
     printf("destroyed message->events[%d]\n", i);
   }
   if (message->events)
     free(message->events);
   free(message);
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

  if (!strcmp(riemann_protocol, "udp"))
    {
    riemann_udp_socket = init_riemann_udp_socket (riemann_server, riemann_port);

    if (riemann_udp_socket == NULL)
       err_quit("[riemann] %s socket failed for %s:%d", riemann_protocol, riemann_server, riemann_port);
  } else if (!strcmp(riemann_protocol, "tcp")) {
    riemann_tcp_socket = init_riemann_tcp_socket (riemann_server, riemann_port);
    if (riemann_tcp_socket == NULL) {
       riemann_circuit_breaker = RIEMANN_CB_OPEN;
       riemann_reset_timeout = apr_time_now () + RIEMANN_RETRY_TIMEOUT * APR_USEC_PER_SEC;
    } else {
       riemann_circuit_breaker = RIEMANN_CB_CLOSED;
       riemann_failures = 0;
    }
  } else {
    err_quit("ERROR: Riemann protocol must be 'udp' or 'tcp'");
  }
/*
  char *keyval[64] = { NULL };
  int toks = tokenize("foo=bar,baz=qux", ",", keyval);
  printf ("keyval tokens %d\n", toks);
  printf ("keyval[0] = %s\n", keyval[0]);
  int i;
  for (i = 0; i < toks; i++)
    free (keyval[i]);
  exit;
*/
/*
  apr_sleep (500 * 1000); // 500ms
*/
  // for (; !done;) {

      riemann_msg = malloc (sizeof (Msg));
      msg__init (riemann_msg);

      /* EVENT 1 */
      // Event *event1 = create_riemann_event ("MyGrid", "clust01", "myhost111", "10.1.1.1", "cpu_system", "100.0", "float", "%", "ok",
      //                     1234567890, "x-loop:1,tag1", "london", 180);

      Event *event1 = create_riemann_event ("MyGrid", "clust01", "myhost111", "10.1.1.1", "cpu_system", "100.0", "float", "%", "ok",
                          1234567890, "", "london", 180);
      riemann_num_events++;
      debug_msg("[riemann] num events = %d\n", riemann_num_events);

      riemann_msg->events = malloc (sizeof (Event) * riemann_num_events);
      riemann_msg->n_events = riemann_num_events;
      riemann_msg->events[riemann_num_events - 1] = event1;

      /* EVENT 2 */
      Event *event2 = create_riemann_event ("MyGrid2", "clust02", "myhost222", "20.2.2.2", "cpu_system", "200.0", "float", "%", "ok",
                          1234567890, "xloop:2,tag2", "london", 180);
      riemann_num_events++;
      debug_msg("[riemann] num events = %d\n", riemann_num_events);
      riemann_msg->events = realloc (riemann_msg->events, sizeof (Event) * riemann_num_events);
      riemann_msg->n_events = riemann_num_events;
      riemann_msg->events[riemann_num_events - 1] = event2;


      debug_msg("[riemann] send %d events in 1 message\n", riemann_num_events);
      send_message_to_riemann(riemann_msg);
      destroy_riemann_msg(riemann_msg);
  // }
}
