import math
from mpi4py import MPI
import sys


def doSearch(splitArr,searchElement,offsetValue,infoDict):
    indexArr=[]
    
    for index,element in enumerate(splitArr):
        if element == searchElement:
            infoDict['indexArr'].append(offsetValue+index)
        elif element >searchElement:
            infoDict['greaterArr'].append(offsetValue+index)
        else:
            infoDict['lesserArr'].append(offsetValue+index)
            
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
    splitArr=[]
    splitArr=disRecData(valArray,WORKING_SIZE,THREAD_COUNT)
    indexArr=[]
    infoDict={'indexArr':[],'greaterArr':[],'lesserArr':[]}
    #logStatement("\n## splitArr for rank: "+str(rank)+"; = "+str(splitArr))
    meshRank=int(math.sqrt(THREAD_COUNT))
    offSetVal=int(rank*WORKING_SIZE)
    indexArr=doSearch(splitArr,SEARCH_ELEMENT,offSetVal,infoDict)
    if rank==THREAD_COUNT-1:
        print("Data distribution completed")
    #print("####rank: ",str(rank),"@@@self indexArr: ",str(indexArr))
    recvArr=[]
    
    #print("@@@@@@@@@MESH RANK: ",meshRank)
    if (rank%meshRank)!=0:
        remNdr=(rank+1)%meshRank
        if remNdr!=0:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
            recvArr=comm.recv(source=rank+1,tag=rank+1)
            #print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+1)
            indexArr=merge(indexArr,recvArr)  
        #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-1)
        comm.send(indexArr,dest=rank-1,tag=rank) 
    else:
        #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+1)
        recvArr=comm.recv(source=rank+1,tag=rank+1)
        #print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+1)
        indexArr=merge(indexArr,recvArr)
        if rank==0:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+meshRank)
            recvArr=comm.recv(source=rank+meshRank,tag=rank+meshRank)
            print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+meshRank)
            indexArr=merge(indexArr,recvArr)
        elif rank==(meshRank*(meshRank-1)):
            #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-meshRank)
            comm.send(indexArr,dest=rank-meshRank,tag=rank)
        else:
            #print(str("####rank: "+str(rank)),"@@@@sourceRank: ",rank+meshRank)
            recvArr=comm.recv(source=rank+meshRank,tag=rank+meshRank)
            print("####rank:",str(rank),"received working copy: ",recvArr, " from proc:",rank+meshRank)
            indexArr=merge(indexArr,recvArr)
            #print(str("####rank: "+str(rank)),"@@@@destRank: ",rank-meshRank)
            comm.send(indexArr,dest=rank-meshRank,tag=rank)
    if rank==0:
        if len(indexArr)!=0:
            print("####rank: ",str(rank),"@@@final indexArr: ",indexArr)
        else:
            print("No instances found in the array")
    #print("SIZEEEEEEEEE: ",comm.Get_size())                         
    #print(str("####rank: "+str(rank)),"process end")


def logStatement(logStr):
    """logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info(logStr)"""
    print(logStr)

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
