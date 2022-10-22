import libchronicle
import sys

path = sys.argv[1]
with libchronicle.Queue(path) as q:
    with q.tailer(0) as tailer:
        while True:
            index, bs = tailer.collect()
            print(f"[{index}] {bs}")
