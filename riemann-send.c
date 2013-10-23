#include <stdio.h>
#include <stdlib.h>
#include "riemann.pb-c.h"

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

    Msg msg = MSG__INIT;
    msg.n_events = 1;
    msg.events = malloc(sizeof(Event) * msg.n_events);
    msg.events[0] = &evt;

    len = msg__get_packed_size(&msg);
    msg_buf = malloc(len);
    msg__pack(&msg,msg_buf);
        
    fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
    fwrite(msg_buf,len,1,stdout); // Write to stdout to allow direct command line piping
        
    free(msg_buf); // Free the allocated serialized buffer
    free(evt_buf); // Free the allocated serialized buffer
    return 0;
}
