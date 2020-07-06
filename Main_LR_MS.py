import os
import math
from subprocess import Popen, PIPE
import sys

#mpiexec -np 3 python ProcessCode.py 1,5,3,4,7,8 2

MAX_THREAD_COUNT=int(sys.argv[1])
CURRENT_THREAD_COUNT=0
INPUT_SIZE=0
WORKING_SIZE=2;
VAL_ARRAY=None


def callProc(remElementCount):
    print("CURRENT_THREAD_COUNT: ",CURRENT_THREAD_COUNT)
    basePath=os.getcwd()
    print(basePath)
    fileName='Process_LR_MS.py'
    print("VAL_ARRAY: ",str(VAL_ARRAY))
    process = Popen(['mpiexec','-np',[str(CURRENT_THREAD_COUNT)],
                     'python',str(basePath+"/"+fileName),
                     [str(WORKING_SIZE)],str(VAL_ARRAY),[str(remElementCount)]],
                    stdout=PIPE, stderr=PIPE)
    while True:
      line = process.stdout.readline()
      err=process.stderr.readline()
      if line:
        #the real code does filtering here
        print("test:", line.rstrip(),flush=True)
        sys.stdout.flush()
      if err:
          print("error: ",err.rstrip(),flush=True)
          sys.stdout.flush()
      if not line:
          break

def main():
    global CURRENT_THREAD_COUNT
    global WORKING_SIZE
    global VAL_ARRAY
    VAL_ARRAY=[8,16,6,19,4]
    print("$$$$valArray: ",VAL_ARRAY)
    INPUT_SIZE=len(VAL_ARRAY)
    print("INPUT_SIZE: ",INPUT_SIZE)
    CURRENT_THREAD_COUNT=MAX_THREAD_COUNT
    WORKING_SIZE=int(INPUT_SIZE/MAX_THREAD_COUNT)
    if CURRENT_THREAD_COUNT%2!=0:
        print("CURRENT_THREAD_COUNT is not even. Ring not possible. Error abort")
        return
    if CURRENT_THREAD_COUNT<4:
        print("CURRENT_THREAD_COUNT is less than 4. Ring not possible. Error abort")
        return
    if WORKING_SIZE<2:
        print("WORKING_SIZE is less than 2. Error abort")
        return
    remElementCount=INPUT_SIZE%(MAX_THREAD_COUNT*WORKING_SIZE)
    print("CURRENT_THREAD_COUNT:",CURRENT_THREAD_COUNT)
    
    callProc(remElementCount)

main()

