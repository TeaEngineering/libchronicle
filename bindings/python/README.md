## Getting started

If you haven't already built the native code to the shared libary `libchronicle.so`), build it with:

    $ git clone git@github.com:TeaEngineering/libchronicle.git
    $ cd libchronicle/native
    $ make

The python bindings for `libchronicle.so` are use the `ctypes` module which is built into python.

One (or more) example [writers](writer.py) can be started with:

    $ cd ../bindings/python/
    $ python3 writer.py /tmp/q4
    chronicle: detected version v0
    hello
    82841329205248
    world
    82841329205249
    ^CTraceback (most recent call last):
      File "/home/chris/repos/libchronicle/bindings/python/writer.py", line 9, in <module>
        for line in sys.stdin:
    KeyboardInterrupt


And the [reading side](reader_cb.py) with:

    $ python3 reader_cb.py /tmp/q4
    chronicle: detected version v5
    [82841329205248] b'hello'
    [82841329205249] b'world'

TODO: publish to pypi
