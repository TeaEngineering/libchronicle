import libchronicle
import os
import sys

path = sys.argv[1]
os.makedirs(path, mode=0o777, exist_ok=True)

with libchronicle.Queue(path, version=5, create=True, roll_scheme="DAILY") as q:
    for line in sys.stdin:
        print(q.append(line.rstrip().encode()))
