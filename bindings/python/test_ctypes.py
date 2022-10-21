from ctypes import *
import os

cx = cdll.LoadLibrary(os.path.expanduser("~/repos/libchronicle/native/obj/libchronicle.so"))

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

cx.chronicle_append.argtypes = [c_void_p, c_char_p]
cx.chronicle_append.restype = c_longlong

cx.chronicle_debug.argtypes = []
q = cx.chronicle_init("/tmp/q1".encode())
cx.chronicle_set_version(q, 5)
cx.chronicle_set_create(q, 1)
cx.chronicle_debug()

assert(cx.chronicle_get_version(q) == 5)
assert(cx.chronicle_set_roll_scheme(q, b"FAST_DAILY") == 0)
assert(cx.chronicle_get_roll_scheme(q) == b"FAST_DAILY")

op = cx.chronicle_open(q)
assert(op == 0)

j = cx.chronicle_append(q, b"hello from python")
print(f'wrote as {j}')

cx.chronicle_debug()

