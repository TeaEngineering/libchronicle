import libchronicle
import sys

def printmsg(ctx, index, bs):
    print(f"[{index}] {bs}")
    return 0

path = sys.argv[1]
with libchronicle.Queue(path) as q:
    tailer = q.tailer(0, cb=printmsg)
    while True:
        q.peek()
