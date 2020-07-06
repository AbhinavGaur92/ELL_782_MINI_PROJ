import math
from mpi4py import MPI
import sys



#print("Load done")

def merge(leftArr,rightArr):
    #logStatement("\n##: In merge : "+str(leftArr)+str(rightArr))
    leftArrLen=len(leftArr)
    rightArrLen=len(rightArr)
    consolArr=[]
    rightArrIndex=0;
    leftArrIndex=0;
    while leftArrIndex<leftArrLen and rightArrIndex<rightArrLen :
        if leftArr[leftArrIndex]<rightArr[rightArrIndex]:
            consolArr.append(leftArr[leftArrIndex])
            leftArrIndex+=1
            if(leftArrIndex==leftArrLen):
                consolArr.extend(rightArr[rightArrIndex:])
                
        else:
            consolArr.append(rightArr[rightArrIndex])
            rightArrIndex+=1
            if(rightArrIndex==rightArrLen):
                consolArr.extend(leftArr[leftArrIndex:])
    #logStatement("\n@@@self consolArr: "+str(consolArr))
    return consolArr

def mergeSort(splitArr):
    remNdr=(len(splitArr))%2
    if remNdr==0:
        splitIndex=int(len(splitArr)/2)
        if splitIndex==1:
            mergedArr=merge(splitArr[splitIndex:],splitArr[:splitIndex])
            return(mergedArr)
        else:
            sortedLeftArr=mergeSort(splitArr[:splitIndex])
            sortedRighttArr=mergeSort(splitArr[splitIndex:])
            mergedArr=merge(sortedLeftArr,sortedRighttArr)
            return(mergedArr)
    else:
         splitIndex=int(len(splitArr)/2)
         if splitIndex==1:
            sortedLeftArr=splitArr[:splitIndex]
            sortedRighttArr=mergeSort(splitArr[splitIndex:])
            mergedArr=merge(sortedLeftArr,sortedRighttArr)
            return(mergedArr)
         else:
            sortedLeftArr=mergeSort(splitArr[:splitIndex])
            sortedRighttArr=mergeSort(splitArr[splitIndex:])
            mergedArr=merge(sortedLeftArr,sortedRighttArr)
            return(mergedArr)

    
def main():
    comm = MPI.COMM_WORLD
    rank = int(comm.Get_rank())
    #logStatement("Process started with rank: "+str(rank))
    valArrayStr=sys.argv[2]
    valArrayStr=valArrayStr[1:-1]
    #logStatement("valArrayStr: "+valArrayStr)
    valArray=[int(val) for val in valArrayStr.split(',')]
    WORKING_SIZE=int(sys.argv[1])
    REM_EL_COUNT=int(sys.argv[3])
    THREAD_COUNT=comm.size
    splitArr=disRecData(valArray,WORKING_SIZE,THREAD_COUNT)
    if rank==THREAD_COUNT-1:
        print("Data distribution completed")
    #logStatement("\n## splitArr for rank: "+str(rank)+"; = "+str(splitArr))
    mergedArr=mergeSort(splitArr)
    #print("####rank: ",str(rank),"@@@self mergedArr: ",str(mergedArr))
    recvArr=None
    meshRank=int(math.sqrt(THREAD_COUNT))
    #print("@@@@@@@@@MESH RANK: ",meshRank)
    if (rank%meshRank)!=0:
        remNdr=(rank+1)%meshRank
        if remNdr!=0:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
            recvArr=comm.recv(source=rank+1,tag=rank+1)
            print("####rank:",str(rank),"received copy: ",recvArr, " from proc:",rank+1)
            mergedArr=merge(mergedArr,recvArr)  
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-1)
        comm.send(mergedArr,dest=rank-1,tag=rank) 
    else:
        #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
        recvArr=comm.recv(source=rank+1,tag=rank+1)
        print("####rank:",str(rank),"received copy: ",recvArr, " from proc:",rank+1)
        mergedArr=merge(mergedArr,recvArr)
        if rank==0:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+meshRank)
            recvArr=comm.recv(source=rank+meshRank,tag=rank+meshRank)
            print("####rank:",str(rank),"received copy: ",recvArr, " from proc:",rank+meshRank)
            mergedArr=merge(mergedArr,recvArr)
            
        elif rank==(meshRank*(meshRank-1)):
            #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-meshRank)
            comm.send(mergedArr,dest=rank-meshRank,tag=rank)
        else:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+meshRank)
            recvArr=comm.recv(source=rank+meshRank,tag=rank+meshRank)
            print("####rank:",str(rank),"received copy: ",recvArr, " from proc:",rank+meshRank)
            mergedArr=merge(mergedArr,recvArr)
            #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-meshRank)
            comm.send(mergedArr,dest=rank-meshRank,tag=rank)
    if rank==0:        
        print("####rank: ",str(rank),"@@@final mergedArr: ",mergedArr,
              " with len: ",len(mergedArr))
    #print("####rank: ",str(rank),"@@@mergedArr: ",str(mergedArr))    
        
    #print("SIZEEEEEEEEE: ",comm.Get_size())          


def logStatement(logStr):
    """logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info(logStr)"""
    print(logStr,flush=True)

def disRecData(valArray,WORKING_SIZE,THREAD_COUNT):
    comm = MPI.COMM_WORLD
    rank = int(comm.Get_rank())
    meshRank=int(math.sqrt(THREAD_COUNT))
    splitArr=[]
    if rank%meshRank==0:
       if rank==0:
        splitArr=valArray[0:WORKING_SIZE]
        #print("####rank:",str(rank),"working copy: ",splitArr)
        del valArray[0:WORKING_SIZE]
        #print("remaining valArray: ",valArray)
        #print("####sideColumn: ",str(rank),valArray[(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE:])
        comm.send(valArray[(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE:],dest=rank+meshRank,tag=rank)
        #print("####upperColumn: ",str(rank),valArray[0:(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE])
        comm.send(valArray[0:(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE],dest=rank+1,tag=rank)
       elif rank==(meshRank-1)*meshRank:
        recv=comm.recv(source=rank-meshRank,tag=rank-meshRank)
        splitArr=recv[0:WORKING_SIZE]
        print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",rank-meshRank)
        del recv[0:WORKING_SIZE]
        #print("####upperColumn: ",str(rank),recv[0:(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE])
        comm.send(recv,dest=rank+1,tag=rank)
       else:
        recv=comm.recv(source=rank-meshRank,tag=rank-meshRank)
        splitArr=recv[0:WORKING_SIZE]
        print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",rank-meshRank)
        del recv[0:WORKING_SIZE]
        #print("remaining valArray: ",valArray)
        #print("####sideColumn: ",str(rank),recv[(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE:])
        comm.send(recv[(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE:],dest=rank+meshRank,tag=rank)
        #print("####upperColumn: ",str(rank),recv[0:(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE])
        comm.send(recv[0:(int(THREAD_COUNT/meshRank)-1)*WORKING_SIZE],dest=rank+1,tag=rank)   
    else:
        remNdr=(rank+1)%meshRank
        recv=comm.recv(source=rank-1,tag=rank-1)
        if remNdr!=0:
            splitArr=recv[0:WORKING_SIZE]
            
            print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",rank-1)
            del recv[0:WORKING_SIZE]
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
            comm.send(recv,dest=rank+1,tag=rank) 
        else:
            splitArr=recv
            print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",rank-1)
            #print("####rank:",str(rank),"working copy: ",splitArr)

    return splitArr
main()
