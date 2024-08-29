from ctypes import (
    c_void_p,
    c_long,
    c_longlong,
    c_int,
    c_char_p,
    POINTER,
    CFUNCTYPE,
    Structure,
    cdll,
    byref,
    string_at,
)
from ctypes.util import find_library
from typing import Optional
import os

lib = find_library("chronicle")
if lib is None:
    # setup default lib location relative to script
    root_path = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    lib = os.path.join(root_path, "native", "obj", "libchronicle.so")

cx = cdll.LoadLibrary(lib)


# typedef struct {
#    COBJ msg;
#    size_t sz;
#    uint64_t index;
# } collected_t;
class Collected(Structure):
    _fields_ = [("msg", c_void_p), ("sz", c_long), ("index", c_longlong)]

#                     ret    arg0      arg1        arg2
TAILER_CB = CFUNCTYPE(c_int, c_void_p, c_longlong, c_char_p)

cx.chronicle_init.argtype = c_char_p
cx.chronicle_init.restype = c_void_p
cx.chronicle_set_version.argtypes = [c_void_p, c_int]
cx.chronicle_set_version.restype = None
cx.chronicle_get_version.argtypes = [c_void_p]
cx.chronicle_get_version.restype = c_int

cx.chronicle_set_create.argtypes = [c_void_p, c_int]
cx.chronicle_set_create.restype = None

cx.chronicle_set_roll_scheme.argtypes = [c_void_p, c_char_p]
cx.chronicle_set_roll_scheme.restype = c_int

cx.chronicle_get_roll_scheme.argtypes = [c_void_p]
cx.chronicle_get_roll_scheme.restype = c_char_p

cx.chronicle_open.argtypes = [c_void_p]
cx.chronicle_open.restype = c_int

cx.chronicle_cleanup.argtypes = [c_void_p]
cx.chronicle_cleanup.restype = c_int

cx.chronicle_append.argtypes = [c_void_p, c_char_p]
cx.chronicle_append.restype = c_longlong

cx.chronicle_tailer.argtypes = [c_void_p, c_void_p, c_void_p, c_longlong]
cx.chronicle_tailer.restype = c_void_p

cx.chronicle_collect.argtypes = [c_void_p, POINTER(Collected)]
cx.chronicle_collect.restype = c_longlong

cx.chronicle_strerror.argtypes = []
cx.chronicle_strerror.restype = c_char_p

cx.chronicle_debug.argtypes = []

cx.chronicle_peek_queue.argtypes = [c_void_p]
cx.chronicle_peek_queue.restype = None

cx.chronicle_peek_tailer.argtypes = [c_void_p]
cx.chronicle_peek_tailer.restype = None


class Queue:
    def __init__(
        self,
        directory: str,
        create: bool = False,
        version: int = 0,
        roll_scheme: Optional[str] = None,
    ):
        self.q = cx.chronicle_init(directory.encode())
        if create:
            cx.chronicle_set_create(self.q, 1)
        if version != 0:
            cx.chronicle_set_version(self.q, version)
        if roll_scheme is not None:
            rc = cx.chronicle_set_roll_scheme(self.q, roll_scheme.encode())
            if rc != 0:
                raise ValueError(f"No such roll_scheme {roll_scheme}")

    def __enter__(self):
        rc = cx.chronicle_open(self.q)
        if rc != 0:
            raise ValueError(f"Open failed: {cx.chronicle_strerror()}")
        return self

    def __exit__(self, type, value, tb):
        rc = cx.chronicle_cleanup(self.q)
        if rc != 0:
            raise ValueError(f"Close failed: {cx.chronicle_strerror()}")

    def debug(self):
        cx.chronicle_debug()

    def append(self, data: bytes):
        return cx.chronicle_append(self.q, data)

    def tailer(self, index: int = 0, cb=None):
        # for now we don't use callback api, just blocking collect
        return Tailer(self, index, cb)

    def peek(self):
        cx.chronicle_peek_queue(self.q)


class Tailer:
    def __init__(self, queue: Queue, index:int = 0, cb=None):
        self.cb_func = None
        if cb:
            self.cb_func = TAILER_CB(cb)
        self.tailer = cx.chronicle_tailer(queue.q, self.cb_func, None, index)
        self.collected = Collected()

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        pass

    def collect(self, timeout=None):
        # TODO: this blocks forever inside the native code, which
        # prevents ctl-C from working
        cx.chronicle_collect(self.tailer, byref(self.collected))
        data = string_at(self.collected.msg, self.collected.sz)
        ## cx.chronicle_return(self.tailer, self.collected)
        return (self.collected.index, data)

    def peek(self):
        cx.chronicle_peek_tailer(self.tailer)
