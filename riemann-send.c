#include <stdio.h>
#include <stdlib.h>
#include "riemann.pb-c.h"

#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>

int main (int argc, const char * argv[]) 
{
//    AMessage msg = AMESSAGE__INIT; // AMessage
    void *evt_buf;                     // Buffer to store serialized data
    void *msg_buf;                     // Buffer to store serialized data
    unsigned len;                  // Length of serialized data
 /*       
    if (argc != 2 && argc != 3)
    {   // Allow one or two integers
        fprintf(stderr,"usage: amessage a [b]\n");
        return 1;
    }
*/
    /* msg.a = atoi(argv[1]);
     if (argc == 3) { msg.has_b = 1; msg.b = atoi(argv[2]); }
     len = amessage__get_packed_size(&msg); */

    Event evt = EVENT__INIT; // AMessage
    evt.state = "ok";
    evt.service = "service111";
    evt.host = "myhost";
    evt.has_metric_sint64 = 1;
    evt.metric_sint64 = 123;

    len = event__get_packed_size(&evt);
    evt_buf = malloc(len);
    event__pack(&evt,evt_buf);

    Msg riemann_msg = MSG__INIT;
    riemann_msg.n_events = 1;
    riemann_msg.events = malloc(sizeof(Event) * riemann_msg.n_events);
    riemann_msg.events[0] = &evt;

    len = msg__get_packed_size(&riemann_msg);
    msg_buf = malloc(len);
    msg__pack(&riemann_msg,msg_buf);
        
    fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
    // fwrite(msg_buf,len,1,stdout); // Write to stdout to allow direct command line piping
/*

   int sockfd;
   struct sockaddr_in *sa_in;
   struct hostent *hostinfo;

   sockfd = socket(AF_INET, SOCK_DGRAM, 0);
   if (sockfd < 0)
      {
         // fprintf(stderr, "create socket (client) %s", strerror(errno));
         return 1;
      }
   memset(&sa_in, 0, sizeof(sa_in));
   sa_in->sin_family = AF_INET;
   sa_in->sin_port = htons (5555);
   hostinfo = gethostbyname ("localhost");
   sa_in->sin_addr = *(struct in_addr *) hostinfo->h_addr;

      int nbytes;

      nbytes = sendto (sockfd, msg_buf, strlen(msg_buf), 0,
                         // (struct sockaddr_in*)&sa_in, sizeof (struct sockaddr_in));
                         (const struct sockaddr *)&sa_in, sizeof (struct sockaddr_in));

      if (nbytes != strlen(msg_buf))
      {
             fprintf(stderr, "sendto socket (client): %s", strerror(errno));
      }
*/

   int sockfd,n;
   struct sockaddr_in servaddr;

   sockfd=socket(AF_INET,SOCK_DGRAM,0);

   bzero(&servaddr,sizeof(servaddr));
   servaddr.sin_family = AF_INET;
   servaddr.sin_addr.s_addr=inet_addr("127.0.0.1");
   servaddr.sin_port=htons(5555);

      sendto(sockfd,msg_buf,strlen(msg_buf),0,
             (struct sockaddr *)&servaddr,sizeof(servaddr));

    free(msg_buf); // Free the allocated serialized buffer
    free(evt_buf); // Free the allocated serialized buffer
    return 0;
}
