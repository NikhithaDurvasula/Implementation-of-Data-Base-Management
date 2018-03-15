package index;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.Minibase;
import global.SearchKey;

/**
 * A HashScan retrieves all records with a given key (via the RIDs of the records).  
 * It is created only through the function openScan() in the HashIndex class. 
 */
public class HashScan implements GlobalConst {

  /** The search key to scan for. */
  protected SearchKey key;

  /** Id of HashBucketPage being scanned. */
  protected PageId curPageId;

  /** HashBucketPage being scanned. */
  protected HashBucketPage curPage;

  /** Current slot to scan from. */
  protected int curSlot;

  // --------------------------------------------------------------------------

  /**
   * Constructs an equality scan by initializing the iterator state.
   */
  protected HashScan(HashIndex index, SearchKey key) {

      /* Use the hash value to find the directory page, if hash value >= MaxEntries
       * then we need to reduce by maxEntries iteratively until the hash value points
       * to some page in the directory(hashvalue < maxEntries) */
      int hashValue = key.getHash(index.DEPTH);
      this.key = new SearchKey(key);
      /* Get the head page of the directory by using the index */
      PageId dirId = new PageId(index.headId.pid);
      HashDirPage dirPage = new HashDirPage();
      /* Start from the head page, and traverse each directory page until hash
       * value < the max entries */
      while(hashValue >= HashDirPage.MAX_ENTRIES) {
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          PageId nextPageId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          dirId = nextPageId;
          hashValue = hashValue - HashDirPage.MAX_ENTRIES;
      }
      /* hash value < max entries, this page should be the directory page needed. */
      Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);

      /* Get the first page id of bucket page */
      this.curPageId = dirPage.getPageId(hashValue);
      Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
      this.curPage = new HashBucketPage();

      if(this.curPageId.pid != INVALID_PAGEID) {
          Minibase.BufferManager.pinPage(this.curPageId, this.curPage, PIN_DISKIO);
          this.curSlot = EMPTY_SLOT;
      }
	  //throw new UnsupportedOperationException("Not implemented");

  } // protected HashScan(HashIndex index, SearchKey key)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {

      if(this.curPageId.pid != INVALID_PAGEID) {
          /* Close the scan, just call the close method */
          close();
      }
	  //throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

  /**
   * Closes the index scan, releasing any pinned pages.
   */
  public void close() {

      /* Unpin the page and set the page to invalid page */
      if (this.curPageId.pid != INVALID_PAGEID) {
          Minibase.BufferManager.unpinPage(this.curPageId, UNPIN_CLEAN);
          this.curPageId.pid = INVALID_PAGEID;
      }
	  //throw new UnsupportedOperationException("Not implemented");

  } // public void close()

    /* #####  this new method is added in the file
     * to test if there is a next bucket ######
     */
    public boolean hasNext()
    {
        while (this.curPageId.pid != INVALID_PAGEID)
        {
            this.curSlot = this.curPage.nextEntry(this.key, this.curSlot);

            if (this.curSlot < 0)
            {
                PageId nextId = this.curPage.getNextPage();
                Minibase.BufferManager.unpinPage(this.curPageId, false);
                this.curPageId = nextId;
                if (this.curPageId.pid != INVALID_PAGEID)
                    Minibase.BufferManager.pinPage(this.curPageId, this.curPage, PIN_DISKIO);
            }
            else
            {
                return true;
            }
        }
        return false;
    }

   /**
   * Gets the next entry's RID in the index scan.
   * 
   * @throws IllegalStateException if the scan has no more entries
   */
  public RID getNext() {

      if (!hasNext()) {
          return null;
          //throw new IllegalStateException("no more elements");
      }
      try {
          return this.curPage.getEntryAt(this.curSlot).rid;
      }catch (IllegalArgumentException exc){
          throw new IllegalStateException ("no next entry");
      }
      //throw new UnsupportedOperationException("Not implemented");

  } // public RID getNext()

} // public class HashScan implements GlobalConst
