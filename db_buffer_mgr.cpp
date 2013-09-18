#include <stdlib.h>
#include "bufmgr.h"

//--------------------------------------------------------------------
// Enumuration for BufMgr Error Handling
//
// Input   : None
// Output  : None
//--------------------------------------------------------------------
enum bufErrCodes {  HASHTBLERROR, HASHNOTFOUND, BUFFEREXCEEDED, INVALIDBUFFERSIZE, 
                              PAGENOTPINNED, PINCOUNTNOTONE, PINCOUNTNOTZERO, INVALIDPAGEID,
                              PAGEWRITEERROR, DEALLOCATERROR, ALLOCATERROR1 };


//--------------------------------------------------------------------
// Constructor for BufMgr
//
// Input   : bufSize  - number of pages in the this buffer manager
// Output  : None
// PostCond: All frames are empty.
//--------------------------------------------------------------------

BufMgr::BufMgr( int bufSize )
{
      if (bufSize < 0)
      {
            MINIBASE_FIRST_ERROR(BUFMGR, INVALIDBUFFERSIZE);
            return;
      }
      
      numBuffers=bufSize;
      
      bufferPool=new frame[numBuffers];      //alloacte a array of frames
      pagePool=new Page[numBuffers];            //alloacte a array of pages
      
      for(int i=0; i<numBuffers; i++){
       bufferPool[i].setFrameNo(i);

      }
      
}


//--------------------------------------------------------------------
// Destructor for BufMgr
//
// Input   : None
// Output  : None
//--------------------------------------------------------------------

BufMgr::~BufMgr()
{   
FlushAllPages();
delete []       bufferPool;
delete []       pagePool;
}


//--------------------------------------------------------------------
// BufMgr::FlushAllPages
//
// Input    : None
// Output   : None
// Purpose  : Flush all pages in the buffer pool to disk.
// Condition: None of the pages in the buffer pool should be pinned.
// PostCond : All dirty pages in the buffer pool are written to 
//            disk (even if some pages are pinned). All frames are empty.
// Return   : OK if operation is successful.  FAIL otherwise.
// Note            : If some pages in the buffer are pinned, this procedure should
//                    flush them to the disk and return FAIL      
//--------------------------------------------------------------------

Status BufMgr::FlushAllPages()
{
      int             frameNumber =-1;
    frame      *chkFrame = 0;

       for(int i=0; i< numBuffers; i++)
       {

               frameNumber=bufferPool[i].getFrameNo();
               
                       if(frameNumber==bufferPool[i].getPinCount() == 1)
                       {
                               MINIBASE_DB->WritePage(frameNumber, chkFrame->getPage());      // Write the pages to MINIBASE
                       }
                                    else{
                                          return FAIL;
                                          }
       }

      return OK;
}


//--------------------------------------------------------------------
// BufMgr::FlushPage
//
// Input    : pid  - page id of a particular page 
// Output   : None
// Purpose  : Flush the page with the given pid to disk.
// Condition: The page with page id = pid must be in the buffer,
//            and should not be pinned. pid cannot be INVALID_PAGE.
// PostCond : The page with page id = pid is written to disk if it is dirty. 
//            The frame where the page resides is empty.
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------


Status BufMgr::FlushPage(PageID pid)
{
      int             frameNo =-1;
    frame      *checkFrame = 0;
  
      frameNo=bufferPool[pid].getFrameNo();
      
      if(frameNo==bufferPool[pid].getPinCount() == 1)
    {
            MINIBASE_DB->WritePage(frameNo, checkFrame->getPage());      // Write the pages to MINIBASE
          return OK;
      }

    else{
                  return FAIL;
      }

} 

//--------------------------------------------------------------------
// BufMgr::PinPage
//
// Input    : pid     - page id of a particular page 
//            isEmpty - (optional, default to FALSE) if true indicate
//                      that the page to be pinned is an empty page.
// Output   : page - a pointer to a page in the buffer pool. (NULL
//            if fail)
// Purpose  : Pin the page with page id = pid to the buffer.  
//            Read the page from disk unless isEmpty is TRUE or
//            the page is already in the buffer.
// Condition: Either the page is already in the buffer, or there is at
//            least one frame available in the buffer pool for the 
//            page.
// PostCond : The page with page id = pid resides in the buffer and 
//            is pinned. The number of pins on the page increase by
//            one.
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------


Status BufMgr::PinPage(PageID pid, Page*& page, bool isEmpty)
{
    int frameNo=-1,a=0;
      frame *checkFrame = 0;
      PageID pageId;
      bool dirty = false;
      Page tempPage;
      //check the page is in the buffer pool
      for(int i=0; i< numBuffers; i++){
                  if(pid==bufferPool[i].getPageID()){
                   numPinRequests++;
                  frameNo=bufferPool[i].getFrameNo();
                  break;
            }

      }

      if(frameNo != 1)
      {
                  // Get the frame
            checkFrame = &bufferPool[frameNo];

                  // Pin the page to this frame
            checkFrame->pin();
            
                  // Retrieve page data
            page = checkFrame->getPage();


      }

      else {
     
                   numPageMisses++;
                  // Check if buffer pool can handle more pages 
                   for(int i=0; i<numBuffers; i++)
            {

                if(bufferPool[i].getPinCount() == 0)
                              a++;
                  if (a == 0 )
                  {
                  page = NULL;
                  MINIBASE_FIRST_ERROR(BUFMGR, BUFFEREXCEEDED);
                  return FAIL;
                  }

            }

                  // Get frame from buffer pool
            
      checkFrame = &bufferPool[frameNo];

            // Get the page id
            pageId = checkFrame->getPageID();

            // Check to see if the frame is dirty. In which case it'll be written back out.
            dirty = checkFrame->getDirtyBit();

                  if (dirty)
            {

                                    // Write the page
                  MINIBASE_DB->WritePage(pageId, checkFrame->getPage());

                              // Increase dirty page written count
                  numDirtyPagesWritten++;
            }

                  if (!isEmpty)
            {
                
                        MINIBASE_DB->ReadPage(pid, &tempPage);                  
            }

            checkFrame->setAndPinPage(pid);
                        // Retrieve page data
            checkFrame->setPage(tempPage);
            page = checkFrame->getPage();
      }

       return OK;
} 

//--------------------------------------------------------------------
// BufMgr::UnpinPage
//
// Input    : pid     - page id of a particular page 
//            dirty   - indicate whether the page with page id = pid
//                      is dirty or not. (Optional, default to FALSE)
// Output   : None
// Purpose  : Unpin the page with page id = pid in the buffer. Mark 
//            the page dirty if dirty is TRUE.  
// Condition: The page is already in the buffer and is pinned.
// PostCond : The page is unpinned and the number of pin on the
//            page decrease by one. 
// Return   : OK if operation is successful.  FAIL otherwise.
//--------------------------------------------------------------------


Status BufMgr::UnpinPage(PageID pid, Bool dirty)
{
      int frameNo = -1;
                  frame *checkFrame = 0;

            frameNo=bufferPool[pid].getFrameNo();

                  if (frameNo == -1)
                  {
      
                        return FAIL;      
                  }

      // Get frame from buffer pool
      checkFrame = &bufferPool[frameNo];



      // Set dirty flag if dirty == TRUE
      if (dirty) 
      {
            checkFrame->setDirtyBit();
      }

      checkFrame->unpin();
    return OK;
}

//--------------------------------------------------------------------
// BufMgr::FreePage
//
// Input    : pid     - page id of a particular page 
// Output   : None
// Purpose  : Free the memory allocated for the page with 
//            page id = pid  
// Condition: Either the page is already in the buffer and is pinned
//            no more than once, or the page is not in the buffer.
// PostCond : The page is unpinned, and the frame where it resides in
//            the buffer pool is freed.  Also the page is deallocated
//            from the database. 
// Return   : OK if operation is successful.  FAIL otherwise.
// Note     : You can call MINIBASE_DB->DeallocatePage(pid) to
//            deallocate a page.
//--------------------------------------------------------------------


Status BufMgr::FreePage(PageID pid)
{
       return OK;
}

//--------------------------------------------------------------------
// BufMgr::NewPage
//
// Input    : howMany - (optional, default to 1) how many pages to 
//                      allocate.
// Output   : pid  - the page id of the first page (as output by
//                   DB::AllocatePage) allocated.
//            page - a pointer to the page in memory.
// Purpose  : Allocate howMany number of pages, and pin the first page
//            into the buffer. 
// Condition: howMany > 0 and there is at least one free buffer space
//            to hold a page.
// PostCond : The page with page id = pid is pinned into the buffer.
// Return   : OK if operation is successful.  FAIL otherwise.
// Note     : You can call MINIBASE_DB->AllocatePage() to allocate a page.  
//            You should call MINIBASE_DB->DeallocatePage() to deallocate the
//            pages you allocate if you failed to pin the page in the
//            buffer. 
//--------------------------------------------------------------------


Status BufMgr::NewPage (int& pid, Page*& page, int howMany)
{
Status stat;


      stat = MINIBASE_DB->AllocatePage(pid, howMany);
      if (stat != OK)
      {
            MINIBASE_FIRST_ERROR(BUFMGR, ALLOCATERROR1);
      
            return FAIL;
      }
      // Try to pin the first page allocated
      stat = PinPage(pid, page);
      if (stat != OK)
      {
                  // Deallocate page
            if (MINIBASE_DB->DeallocatePage(pid, howMany) != OK)
            {
                  MINIBASE_FIRST_ERROR(BUFMGR, DEALLOCATERROR);
                  return FAIL;
            }

      }

      return OK;
}


//--------------------------------------------------------------------
// BufMgr::GetNumOfUnpinnedBuffers
//
// Input    : None
// Output   : None
// Purpose  : Find out how many unpinned locations are in the buffer
//            pool.
// Condition: None
// PostCond : None
// Return   : The number of unpinned buffers in the buffer pool.
//--------------------------------------------------------------------

unsigned int BufMgr::GetNumOfUnpinnedBuffers()
{
      return 0;      
}


//--------------------------------------------------------------------
// BufMgr::GetNumOfBuffers
//
// Input    : None
// Output   : None
// Purpose  : Find out how many buffers are there in the buffer pool.
// PreCond  : None
// PostCond : None
// Return   : The number of buffers in the buffer pool.
//--------------------------------------------------------------------

unsigned int BufMgr::GetNumOfBuffers()
{
    return 0;
}





//--------------------------------------------------------------------
// BufMgr::ResetStat
//
// Input    : None 
// Output   : None
// Purpose  : Reset the statistic variables
//--------------------------------------------------------------------

Status BufMgr::ResetStat( )
{
      return OK;
}


//--------------------------------------------------------------------
// BufMgr::PrintStat
//
// Input    : None 
// Output   : None
// Purpose  : Print the statistic variables
//--------------------------------------------------------------------

Status BufMgr::PrintStat( )
{
      return OK;      
}


