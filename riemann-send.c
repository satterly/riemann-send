#include <stdio.h>
#include <stdlib.h>
#include "riemann.pb-c.h"

#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>

int
tokenize(char *str, char *delim, char **splitstr)
{      
  char *p;      
  int i=0;      

  p = strtok(str, delim);      
  while(p!= NULL)      
  {                
    printf("%s", p);
    splitstr[i] = malloc(strlen(p) + 1);
    if (splitstr[i])
      strcpy(splitstr[i], p);
    i++;
    p = strtok (NULL, delim);       
  } 
  return i++;
}

int main (int argc, const char * argv[]) 
{
//    AMessage msg = AMESSAGE__INIT; // AMessage
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
    evt.time = 1234567890;
    evt.state = "ok";
    evt.service = "service111";
    evt.host = "myhost";
    evt.description = "this is the description";

    // char *tags[] = { "one", "two", "three", NULL };
    char raw_tags[80] = "cat=dog,length=1,wibble";
    printf("raw tags = %s\n", raw_tags);

   /*
    int n_tags = 0;
    char *token;

    token = strtok(raw_tags, ",");
    if (token) {
        printf("tag %d %s\n", n_tags, token); 
        char *tags = malloc(sizeof(token));
        
        for (n_tags = 1; token = strtok(NULL, ","); n_tags++ ) {
           printf("tags %d %s\n", n_tags, token); 
    
        }
    }
    evt.n_tags = n_tags++;
    evt.tags =  tags;
 */
    int n_tags;
    char *tags[64] = { NULL };

    evt.n_tags = tokenize(raw_tags, ",", tags);
    evt.tags = tags;

    char *raw_attrs = "environment=PROD,grid=MyGrid";

    Attribute attrs = ATTRIBUTE__INIT;
    attrs.key = "foo";
    attrs.value = "bar";

    evt.ttl = 86400;

    evt.n_attributes = 1;
    evt.attributes = malloc(sizeof(Attribute) * evt.n_attributes);
    evt.attributes[0] = &attrs;

    evt.has_metric_sint64 = 1;
    evt.metric_sint64 = 123;

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
    return 0;
}
