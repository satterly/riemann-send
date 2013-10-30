riemann-send
============

Installation
------------

    $ git clone https://github.com/satterly/riemann-send.git
    $ cd riemann-send
    $ sudo apt-get install protobuf-c-compiler libprotobuf-c0-dev
    $ protoc-c --c_out=. riemann.proto
    $ gcc -g -o riemann-send riemann.pb-c.c riemann-send.c -lprotobuf-c

Example
-------

    $ ./riemann-send

```
> cat
> foo=bar
> yes=yay!
> env=PROD
> grid=MyGrid
> location=paris
> foo=bar
buffer[0] = env=PROD
> env
> PROD
attributes[0] -> key = env value = PROD
buffer[1] = grid=MyGrid
> grid
> MyGrid
attributes[1] -> key = grid value = MyGrid
buffer[2] = location=paris
> location
> paris
attributes[2] -> key = location value = paris
buffer[3] = foo=bar
> foo
> bar
attributes[3] -> key = foo value = bar
n_attrs = 4
tag 0 cat
tag 1 foo=bar
tag 2 yes=yay!
Writing 118 serialized bytes
```
    
    
References
----------

[1] https://code.google.com/p/protobuf-c/wiki/Examples
