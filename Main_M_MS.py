import math
from subprocess import Popen, PIPE
import os
import sys


#mpiexec -np 3 python ProcessCode.py 1,5,3,4,7,8 2
import sys
MAX_THREAD_COUNT=int(sys.argv[1])
CURRENT_THREAD_COUNT=0
INPUT_SIZE=0
WORKING_SIZE=0;
VAL_ARRAY=None


def callProc(remElementCount):
    print("CURRENT_THREAD_COUNT: ",CURRENT_THREAD_COUNT)
    basePath=os.getcwd()
    print(basePath)
    fileName='Process_M_MS.py'
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
    VAL_ARRAY=[8,16,6,2,4,9,3,8,7]
    print("$$$$valArray: ",VAL_ARRAY)
    INPUT_SIZE=len(VAL_ARRAY)
    print("INPUT_SIZE: ",INPUT_SIZE)
    CURRENT_THREAD_COUNT=MAX_THREAD_COUNT
    if CURRENT_THREAD_COUNT<4:
        print("CURRENT_THREAD_COUNT is less than 4. Mesh not possible. Error abort")
        return
    CURRENT_RANK=2
    while True:
        TEMP_COUNT=CURRENT_RANK*CURRENT_RANK
        TEMP_SIZE=int(INPUT_SIZE/TEMP_COUNT)
        if TEMP_SIZE<2 or TEMP_COUNT>MAX_THREAD_COUNT:
            break;
        else:
            WORKING_SIZE=TEMP_SIZE
            CURRENT_THREAD_COUNT=TEMP_COUNT
            CURRENT_RANK=CURRENT_RANK+1       
            
              
    remElementCount=INPUT_SIZE%(CURRENT_THREAD_COUNT*WORKING_SIZE)
    """print("remElementCount: ",remElementCount)
    print("WORKING_SIZE: ",WORKING_SIZE)"""
    print("CURRENT_THREAD_COUNT:",CURRENT_THREAD_COUNT)
    
    callProc(remElementCount)
    
    print("\nDone")

main()

