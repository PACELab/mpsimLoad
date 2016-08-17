# mpsimLoad
Load generation tool for a memcached tier backed with a database tier.

usage: mpsimLoad.py [-h] [-c C] [-n N] [-l L] [-iafile IAFILE]

Load generation script for memcached, ardb setup

optional arguments:
  -h, --help      show this help message and exit
  -c C            Concurrency, number of threads
  -n N            Total requests to send
  -l L            When specified, used as max request rate
  -iafile IAFILE  When specified, uses file for request rates -l option is
                  ignored
