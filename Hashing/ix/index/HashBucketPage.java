package index;

import global.Minibase;
import global.PageId;

/**
 * An object in this class is a page in a linked list.
 * The entire linked list is a hash table bucket.
 */
class HashBucketPage extends SortedPage {

  /**
   * Gets the number of entries in this page and later
   * (overflow) pages in the list.
   * <br><br>
   * To find the number of entries in a bucket, apply 
   * countEntries to the primary page of the bucket.
   */
  public int countEntries() {

      int numOfEntry = getEntryCount();
      PageId dataId = getNextPage();
      HashBucketPage nextPage = new HashBucketPage();
      /* Find all valid pages and sum the entry count of each page */
      while(dataId.pid != INVALID_PAGEID) {
          Minibase.BufferManager.pinPage(dataId, nextPage, PIN_DISKIO);
          numOfEntry += nextPage.getEntryCount();
          PageId nextId = nextPage.getNextPage();
          Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
          dataId = nextId;
      }

      return numOfEntry;
	  //throw new UnsupportedOperationException("Not implemented");

  } // public int countEntries()

  /**
   * Inserts a new data entry into this page. If there is no room
   * on this page, recursively inserts in later pages of the list.  
   * If necessary, creates a new page at the end of the list.
   * Does not worry about keeping order between entries in different pages.
   * <br><br>
   * To insert a data entry into a bucket, apply insertEntry to the
   * primary page of the bucket.
   * 
   * @return true if inserting made this page dirty, false otherwise
   */
  public boolean insertEntry(DataEntry entry) {

      try {
          /* if the page is not full, just call insertEntry in SortedPage
           * to insert the entry into the page. */
          super.insertEntry(entry);
          return true;
      }
      catch(IllegalStateException illegalstate) {
          /* if the page is full or not enough space for new entry, check
           * the next page if the next page is valid, just insert entry in
           * this page, if not valid, add a new page in the list */
          HashBucketPage nextPage = new HashBucketPage();
          PageId nextPageId = getNextPage();
          if (nextPageId.pid != INVALID_PAGEID)
          {
              Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);

              boolean dirty = nextPage.insertEntry(entry);
              Minibase.BufferManager.unpinPage(nextPageId, dirty);
              return false;
          }
          /* create a new page for new entry and add it to the list */
          nextPageId = Minibase.BufferManager.newPage(nextPage, 1);
          setNextPage(nextPageId);
          /* Insert the entry into the page, if successful, write it to disk. */
          boolean dirty = nextPage.insertEntry(entry);
          Minibase.BufferManager.unpinPage(nextPageId, dirty);
          return true;
      }
	  //throw new UnsupportedOperationException("Not implemented");

  } // public boolean insertEntry(DataEntry entry)

  /**
   * Deletes a data entry from this page.  If a page in the list 
   * (not the primary page) becomes empty, it is deleted from the list.
   * 
   * To delete a data entry from a bucket, apply deleteEntry to the
   * primary page of the bucket.
   * 
   * @return true if deleting made this page dirty, false otherwise
   * @throws IllegalArgumentException if the entry is not in the list.
   */
  public boolean deleteEntry(DataEntry entry) {

      try {
          /* delete a data entry from bucket */
          super.deleteEntry(entry);
          return true;
      }
      catch(IllegalArgumentException illegalstate) {
          /* if the entry does not exist in the page, find it in the next page
           * recursively until the entry found. */
          HashBucketPage nextPage = new HashBucketPage();
          PageId nextPageId = getNextPage();

          if (nextPageId.pid != INVALID_PAGEID)
          {
              Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);
              boolean dirty = nextPage.deleteEntry(entry);
              /* if the page becomes empty, delete that page */
              if (nextPage.getEntryCount() < 1)
              {
                  /* adjust the pointer to the page after the deleted page */
                  setNextPage(nextPage.getNextPage());
                  /* if successful, write to disk and free the page */
                  Minibase.BufferManager.unpinPage(nextPageId, dirty);
                  Minibase.BufferManager.freePage(nextPageId);
                  return true;
              }

              Minibase.BufferManager.unpinPage(nextPageId, dirty);
              return false;
          }

          throw illegalstate;
      }
      //throw new UnsupportedOperationException("Not implemented");

  } // public boolean deleteEntry(DataEntry entry)

} // class HashBucketPage extends SortedPage
