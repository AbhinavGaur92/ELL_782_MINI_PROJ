from mpi4py import MPI
import sys

def doSearch(splitArr,searchElement,offsetValue,infoDict):
    #print("in doSearch")
    indexArr=[]
    
    for index,element in enumerate(splitArr):
        if element == searchElement:
            infoDict['indexArr'].append(offsetValue+index)
        elif element >searchElement:
            infoDict['greaterArr'].append(offsetValue+index)
        else:
            infoDict['lesserArr'].append(offsetValue+index)
            
            
    #print("self indexArr: ",infoDict)
    return infoDict


def merge(infoDict,recvDict):
    #print("indexArr,recvArr: ",infoDict,recvDict)
    for key in infoDict.keys():
        infoDict[key].extend(recvDict[key])
    #print("infoDict,recvDict: ",infoDict,recvDict)
    return infoDict

def main():
    comm = MPI.COMM_WORLD
    rank = int(comm.Get_rank())
    valArrayStr=sys.argv[2]
    valArrayStr=valArrayStr[1:-1]
    #logStatement("valArrayStr: "+valArrayStr)
    valArray=[int(val) for val in valArrayStr.split(',')]
    WORKING_SIZE=int(sys.argv[1])
    REM_EL_COUNT=int(sys.argv[3])
    SEARCH_ELEMENT=int(sys.argv[4])
    THREAD_COUNT=comm.size
    #splitArr=valArray
    splitArr=disRecData(valArray,WORKING_SIZE,THREAD_COUNT)
    #return
    indexArr=[]
    infoDict={'indexArr':[],'greaterArr':[],'lesserArr':[]}

    #logStatement("\n## splitArr for rank: "+str(rank)+"; = "+str(splitArr))
    offSetVal=0
    if rank==0:
        offsetVal=0
    elif rank >(THREAD_COUNT/2):
        offSetVal=(THREAD_COUNT-rank)*WORKING_SIZE
    else:
        offSetVal=(rank)*WORKING_SIZE+int(((THREAD_COUNT/2)-1)*WORKING_SIZE)
    #logStatement("\n## offSetVal for rank: "+str(rank)+" = "+str(offSetVal))
    indexArr=doSearch(splitArr,SEARCH_ELEMENT,offSetVal,infoDict)
    if rank==int(((THREAD_COUNT/2))):
        print("Data distribution completed")
    #print("####rank:",str(rank),"@@@splitArr: ",str(splitArr))
    recvArr=[]
    if rank==0:
        #print(str("####rank:"+str(rank)),"@@@@sourceRank: ",THREAD_COUNT-1)
        recvArr=comm.recv(source=THREAD_COUNT-1,tag=THREAD_COUNT-1)
        print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",THREAD_COUNT-1)
        indexArr=merge(indexArr,recvArr)
        if THREAD_COUNT>2:
            #print(str("####rank:"+str(rank)),"@@@@sourceRank: ",rank+1)
            recvArr=comm.recv(source=rank+1,tag=rank+1)
            print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+1)
            indexArr=merge(indexArr,recvArr)
    elif rank==(THREAD_COUNT)/2:
        #print(str("####rank:"+str(rank)),"@@@@destRank: ",rank-1)
        comm.send(indexArr,dest=rank-1,tag=rank)
    elif rank==(THREAD_COUNT/2)+1 and rank!=THREAD_COUNT-1:
        #print(str("####rank:"+str(rank)),"@@@@destRank: ",rank+1)
        comm.send(indexArr,dest=rank+1,tag=rank)    
    elif rank > ((THREAD_COUNT/2)+1) and rank < (THREAD_COUNT-1):
        #print(str("####rank:"+str(rank)),"@@@@sourceRank: ",rank-1)
        recvArr=comm.recv(source=rank-1,tag=rank-1)
        print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank-1)
        indexArr=merge(indexArr,recvArr)
        #print(str("####rank:"+str(rank)),"@@@@destRank: ",rank+1)
        comm.send(indexArr,dest=rank+1,tag=rank)
    elif rank < (THREAD_COUNT/2) and rank >0:
        #print(str("####rank:"+str(rank)),"@@@@sourceRank: ",rank+1)
        recvArr=comm.recv(source=rank+1,tag=rank+1)
        print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+1)
        indexArr=merge(indexArr,recvArr) 
        #print(str("####rank:"+str(rank)),"@@@@destRank: ",rank-1)
        comm.send(indexArr,dest=rank-1,tag=rank)
    elif rank==(THREAD_COUNT-1):
        if THREAD_COUNT>4:
            #print(str("####rank:"+str(rank)),"@@@@sourceRank: ",rank-1)
            recvArr=comm.recv(source=rank-1,tag=rank-1)
            print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank-1)
            indexArr=merge(indexArr,recvArr)
        #print(str("####rank:"+str(rank)),"@@@@destRank: ",0)
        comm.send(indexArr,dest=0,tag=rank) 
    else:print("Do nothing")
    if rank==0:
        if len(indexArr)!=0:
            print("####rank:",str(rank),"@@@final indexArr: ",indexArr)
        else:
            print("No instances found in the array")     


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
        del valArray[0:WORKING_SIZE]
        #print("remaining valArray: ",valArray)
        #print("####upperHalfrank: ",str(rank),valArray[(int(THREAD_COUNT/2)-1)*WORKING_SIZE:])
        comm.send(valArray[(int(THREAD_COUNT/2)-1)*WORKING_SIZE:],dest=rank+1,tag=rank)
        #print("####lower halfrank: ",str(rank),valArray[0:(int(THREAD_COUNT/2)-1)*WORKING_SIZE])
        comm.send(valArray[0:(int(THREAD_COUNT/2)-1)*WORKING_SIZE],dest=THREAD_COUNT-1,tag=rank)
        #comm.send(valArray[0:(int(THREAD_COUNT/2)-1)*WORKING_SIZE],dest=rank+1,tag=rank)
        #comm.send(valArray[(int(THREAD_COUNT/2)-1)*WORKING_SIZE:],dest=THREAD_COUNT-1,tag=rank)

        
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
        print("####rank:",str(rank),"received working copy: ",recv," from proc:",rank-1)
        splitArr=recv
    
    #print("####rank:",str(rank),"working copy: ",splitArr)    
    return splitArr

main()
