from mpi4py import MPI
import sys


#print("Load done")

WORKING_SIZE=0;


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
    #logStatement("\n@@@consolArr: "+str(consolArr))
    return consolArr

def mergeSort(splitArr):
    if len(splitArr)==0:
        return splitArr
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
    splitArr=[]
    splitArr=disRecData(valArray,WORKING_SIZE,THREAD_COUNT)
    if rank==int(((THREAD_COUNT/2))):
        print("Data distribution completed")
    offSetVal=0
    if rank >(THREAD_COUNT/2)-1:
        offSetVal=int(((THREAD_COUNT/2)+1)*WORKING_SIZE)
    else:
        offSetVal=int(rank*WORKING_SIZE)
    mergedArr=mergeSort(splitArr)
    #print("####rank: ",str(rank),"@@@self mergedArr: ",str(mergedArr))
    recvArr=[]
    if rank==0:
        #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",THREAD_COUNT-1)
        recvArr=comm.recv(source=THREAD_COUNT-1,tag=THREAD_COUNT-1)
        mergedArr=merge(mergedArr,recvArr)
        if THREAD_COUNT>2:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
            recvArr=comm.recv(source=rank+1,tag=rank+1)
            print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+1)
            mergedArr=merge(mergedArr,recvArr)
            print("####rank: ",str(rank),"@@@final mergedArr: ",str(mergedArr))         
    elif rank==(THREAD_COUNT)/2:
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-1)
        comm.send(mergedArr,dest=rank-1,tag=rank)
    elif rank==(THREAD_COUNT/2)+1 and rank!=THREAD_COUNT-1:
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank+1)
        comm.send(mergedArr,dest=rank+1,tag=rank)    
    elif rank > ((THREAD_COUNT/2)+1) and rank < (THREAD_COUNT-1):
        #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank-1)
        recvArr=comm.recv(source=rank-1,tag=rank-1)
        #print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank-1)
        mergedArr=merge(mergedArr,recvArr)
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank+1)
        comm.send(mergedArr,dest=rank+1,tag=rank)
    elif rank < (THREAD_COUNT/2) and rank >0:
        #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
        recvArr=comm.recv(source=rank+1,tag=rank+1)
        #print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+1) 
        mergedArr=merge(mergedArr,recvArr)  
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-1)
        comm.send(mergedArr,dest=rank-1,tag=rank)
    elif rank==(THREAD_COUNT-1):
        if THREAD_COUNT>4:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank-1)
            recvArr=comm.recv(source=rank-1,tag=rank-1)
            #print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank-1)
            mergedArr=merge(mergedArr,recvArr)
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",0)
        comm.send(mergedArr,dest=0,tag=rank) 
    else:print("Do nothing")
    #print("####rank: ",str(rank),"@@@mergedArr: ",str(mergedArr))
    #print("SIZEEEEEEEEE: ",comm.Get_size())                         
    #print(str("####rank: "+str(rank)),"for loop end")


def logStatement(logStr):
    """logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info(logStr)"""
    print(logStr)



def disRecData(valArray,WORKING_SIZE,THREAD_COUNT):
    comm = MPI.COMM_WORLD
    rank = int(comm.Get_rank())
    splitArr=[]
    if rank==0:
        splitArr=valArray[0:WORKING_SIZE]
        print("####rank:",str(rank),"working copy: ",splitArr)
        del valArray[0:WORKING_SIZE]
        #print("remaining valArray: ",valArray)
        #print("####upperHalfrank: ",str(rank),valArray[(int(THREAD_COUNT/2)-1)*WORKING_SIZE:])
        comm.send(valArray[(int(THREAD_COUNT/2)-1)*WORKING_SIZE:],dest=rank+1,tag=rank)
        #print("####lower halfrank: ",str(rank),valArray[0:(int(THREAD_COUNT/2)-1)*WORKING_SIZE])
        comm.send(valArray[0:(int(THREAD_COUNT/2)-1)*WORKING_SIZE],dest=THREAD_COUNT-1,tag=rank)
        
    elif rank==THREAD_COUNT-1:
        recv=comm.recv(source=0,tag=0)
        splitArr=recv[0:WORKING_SIZE]
        del recv[0:WORKING_SIZE]
        print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",0)
        comm.send(recv,dest=rank-1,tag=rank)
    elif rank>(THREAD_COUNT/2) and rank < THREAD_COUNT-1:
        recv=comm.recv(source=rank+1,tag=rank+1)
        splitArr=recv[0:WORKING_SIZE]
        del recv[0:WORKING_SIZE]
        if THREAD_COUNT>4:
            print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",rank+1)
            comm.send(recv,dest=rank-1,tag=rank)
    elif rank<=(THREAD_COUNT/2)-1 and rank >0:
        recv=comm.recv(source=rank-1,tag=rank-1)
        splitArr=recv[0:WORKING_SIZE]
        del recv[0:WORKING_SIZE]
        print("####rank:",str(rank),"received working copy: ",splitArr, " from proc:",rank+1)
        comm.send(recv,dest=rank+1,tag=rank)
    elif rank==(THREAD_COUNT/2):
        recv=comm.recv(source=rank-1,tag=rank-1)
        print("####rank:",str(rank),"received working copy: ",recv," from proc:",rank+1)
        splitArr=recv
    return splitArr

main()
