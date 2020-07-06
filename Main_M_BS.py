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
SEARCH_ELEMENT=int(sys.argv[2])

def callProc(remElementCount):
    print("CURRENT_THREAD_COUNT: ",CURRENT_THREAD_COUNT)
    basePath=os.getcwd()
    print(basePath)
    fileName='Process_M_BS.py'
    print("VAL_ARRAY: ",str(VAL_ARRAY))
    process = Popen(['mpiexec','-np',[str(CURRENT_THREAD_COUNT)],
                     'python',str(basePath+"/"+fileName),
                     [str(WORKING_SIZE)],str(VAL_ARRAY),[str(remElementCount)]
                     ,str(SEARCH_ELEMENT)],
                    stdout=PIPE, stderr=PIPE)
    while True:
      sys.stdout.flush()  
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
    VAL_ARRAY=[8,16,6,19,4,1,3,17,7,9,11,8,25,22,31,29,
               15,12,21,13,8,44,43,8,26,23,5,27,28,8,47,77,49,
               58,8,47,1,9,44,3,2,8,7,22,45,67,90,97,30,24,65,78]
    print("$$$$valArray: ",VAL_ARRAY)
    INPUT_SIZE=len(VAL_ARRAY)
    print("INPUT_SIZE: ",INPUT_SIZE)
    CURRENT_THREAD_COUNT=MAX_THREAD_COUNT
    if CURRENT_THREAD_COUNT<4:
        print("CURRENT_THREAD_COUNT is less than 4. Ring not possible. Error abort")
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

