import libchronicle
import os
import sys
import random
import string
from time import sleep

path = sys.argv[1]
os.makedirs(path, mode=0o777, exist_ok=True)

with libchronicle.Queue(path, version=5, create=True, roll_scheme="TEST_SECONDLY") as q:
    for i in range(int(1e8)):
        N = random.randrange(1, 10000)
        line = "".join(random.choices(string.ascii_uppercase + string.digits, k=N))
        print(q.append(line.rstrip().encode()))
        sleep(random.randrange(1, 100) / 1000.0)
