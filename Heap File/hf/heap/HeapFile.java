package heap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * <h3>Minibase Heap Files</h3>
 * A heap file is the simplest database file structure.  It is an unordered 
 * set of records, stored on a set of data pages. <br>
 * This class supports inserting, selecting, updating, and deleting
 * records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other
 * relational operators. A temporary heap file does not have an entry in the
 * file library and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class)
 * is the most basic access method.
 */
public class HeapFile implements GlobalConst {

  /** HFPage type for directory pages. */
  protected static final short DIR_PAGE = 10;

  /** HFPage type for data pages. */
  protected static final short DATA_PAGE = 11;

  // --------------------------------------------------------------------------

  /** Is this a temporary heap file, meaning it has no entry in the library? */
  protected boolean isTemp;

  /** The heap file name.  Null if a temp file, otherwise 
   * used for the file library entry. 
   */
  protected String fileName;

  /** First page of the directory for this heap file. */
  protected PageId headId;

  // --------------------------------------------------------------------------

  /**
   * If the given name is in the library, this opens the corresponding
   * heapfile; otherwise, this creates a new empty heapfile. 
   * A null name produces a temporary file which
   * requires no file library entry.
   */
  public HeapFile(String name) {

    fileName = name;

    if (name != null) {
      isTemp = false;
      PageId pageId = Minibase.DiskManager.get_file_entry(name);

      if (pageId != null) {
        headId = pageId;
      }
      else {
        createNewHF();
      }
    }
    else { //This creates a new Temporary heap file
      isTemp = true;
      createNewHF();
    }
    //throw new UnsupportedOperationException("Not implemented");

  } // public HeapFile(String name)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the heap file if it's temporary.
   */
  protected void finalize() throws Throwable {

    if (isTemp)
      deleteFile();
    //throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

  /**
   * Deletes the heap file from the database, freeing all of its pages
   * and its library entry if appropriate.
   */
  public void deleteFile() {

    PageId dirId = new PageId();
    DirPage dirPage = new DirPage();

    // We will read the head of directory page
    Minibase.BufferManager.pinPage(headId, dirPage, PIN_DISKIO);

    do {
      // Here we go to the data pages referred in this directory and will free it by freePage method
      for (int i = 0; i < dirPage.getEntryCnt(); i++) {
        Minibase.BufferManager.freePage(dirPage.getPageId(i));
      }
// we go to the next page in the directory
      dirId = dirPage.getNextPage();
      Minibase.BufferManager.unpinPage(dirPage.getCurPage(), UNPIN_CLEAN);
      Minibase.BufferManager.freePage(dirPage.getCurPage());

      if (dirId.pid != INVALID_PAGEID) {
        Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
      }
    } while (dirId.pid != INVALID_PAGEID);
    //throw new UnsupportedOperationException("Not implemented");

  } // public void deleteFile()

  /**
   * Inserts a new record into the file and returns its RID.
   * Should be efficient about finding space for the record.
   * However, fixed length records inserted into an empty file
   * should be inserted sequentially.
   * Should create a new directory and/or data page only if
   * necessary.
   *
   * @throws IllegalArgumentException if the record is too
   * large to fit on one data page
   */
  public RID insertRecord(byte[] record) throws IllegalArgumentException {

    HFPage hfPage = new HFPage();

    // Create another page where the record length is sufficient
    PageId anotherPageId = getAvailPage(record.length);
    Minibase.BufferManager.pinPage(anotherPageId, hfPage, PIN_DISKIO);

    RID recordId = hfPage.insertRecord(record);

    // Updating the directory entry point
    updateDirEntry(anotherPageId, 1, hfPage.getFreeSpace());

    Minibase.BufferManager.unpinPage(anotherPageId, UNPIN_DIRTY);

    return recordId;

    //throw new UnsupportedOperationException("Not implemented");

  } // public RID insertRecord(byte[] record)

  /**
   * Reads a record from the file, given its rid.
   *
   * @throws IllegalArgumentException if the rid is invalid
   */
  public byte[] selectRecord(RID rid) throws IllegalArgumentException {

    HFPage dataPage = new HFPage();
    byte[] record;

    // Pin the data page so that we can select the record
    Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);

    // Will throw IllegalArgumentException if the rid is invalid
    record = dataPage.selectRecord(rid).clone();

    // Unpin the data page to since we are done with it
    Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);

    return record;
    //throw new UnsupportedOperationException("Not implemented");
  } // public byte[] selectRecord(RID rid)

  /**
   * Updates the specified record in the heap file.
   *
   * @throws IllegalArgumentException if the rid or new record is invalid
   */
  public void updateRecord(RID rid, byte[] newRecord) throws IllegalArgumentException {

    HFPage dataPage = new HFPage();

    // Pin the data page so that we can update the record
    Minibase.BufferManager.pinPage(rid.pageno, dataPage, PIN_DISKIO);

    // Update the record with the newRecord
    // Will throw IllegalArgumentException if the rid is invalid or input record's length is different
    dataPage.updateRecord(rid, newRecord);

    // Unpin the data page to save the changes
    Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
    //throw new UnsupportedOperationException("Not implemented");

  } // public void updateRecord(RID rid, byte[] newRecord)

  /**
   * Deletes the specified record from the heap file.
   * Removes empty data and/or directory pages.
   *
   * @throws IllegalArgumentException if the rid is invalid
   */
  public void deleteRecord(RID rid) throws IllegalArgumentException {

    HFPage hfPage = new HFPage();
    Minibase.BufferManager.pinPage(rid.pageno, hfPage, PIN_DISKIO);
    hfPage.deleteRecord(rid);
    int freeSpace = hfPage.getFreeSpace();
    //Delete the invalid records and re-save the spae that is free and then unpin the changes.
    Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
    updateDirEntry(rid.pageno, -1, freeSpace);
    //throw new UnsupportedOperationException("Not implemented");

  } // public void deleteRecord(RID rid)

  /**
   * Gets the number of records in the file.
   */
  public int getRecCnt() {

    DirPage directoryPage = new DirPage();
    PageId nextDirectoryPageId = new PageId();
    int count = 0;
    Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO);

    do {
      for (int i = 0; i < directoryPage.getEntryCnt(); i++) {
        count = count + directoryPage.getRecCnt(i);
      }

      nextDirectoryPageId = directoryPage.getNextPage();

      if (INVALID_PAGEID != nextDirectoryPageId.pid) {
        Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
        Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);
      }
    } while (nextDirectoryPageId.pid != INVALID_PAGEID);

    Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
    return count;
    //throw new UnsupportedOperationException("Not implemented");

  } // public int getRecCnt()

  /**
   * Initiates a sequential scan of the heap file.
   */
  public HeapScan openScan() {
    return new HeapScan(this);
  }

  /**
   * Returns the name of the heap file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Searches the directory for the first data page with enough free space to store a
   * record of the given size. If no suitable page is found, this creates a new
   * data page.
   * A more efficient implementation would start with a directory page that is in the
   * buffer pool.
   */
  protected PageId getAvailPage(int reclen) {

    DirPage directoryPage = new DirPage();
    PageId nextDirectoryPageId = new PageId();

    // 20 is the header size and 4 is the slot size
    if (reclen > PAGE_SIZE - 20 - 4) {
      throw new IllegalArgumentException("Size of the record is more than the size of the page");
    }

    Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO);

    do {
      for (int i = 0; i < directoryPage.getEntryCnt(); i++) {
        if (directoryPage.getFreeCnt(i) >= reclen + 4) {
          PageId availablePageId = directoryPage.getPageId(i);
          Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
          return availablePageId;
        }
      }
      nextDirectoryPageId = directoryPage.getNextPage();

      if (nextDirectoryPageId.pid != INVALID_PAGEID) {
        Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
        Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);
      }
    } while (nextDirectoryPageId.pid != INVALID_PAGEID);

    Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
    return insertPage();
    //throw new UnsupportedOperationException("Not implemented");

  } // protected PageId getAvailPage(int reclen)

  /**
   * Helper method for finding directory entries of data pages.
   * A more efficient implementation would start with a directory
   * page that is in the buffer pool.
   * @param pageno identifies the page for which to find an entry
   * @param dirId output param to hold the directory page's id (pinned)
   * @param dirPage output param to hold directory page contents
   * @return index of the data page's entry on the directory page
   */
  protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {

    PageId nextDirectoryPageId = new PageId();
    Minibase.BufferManager.pinPage(headId, dirPage, PIN_DISKIO);

    do {
      for (int i = 0; i < dirPage.getEntryCnt(); i++) {
        if (pageno.pid == dirPage.getPageId(i).pid) {
          dirId.pid = dirPage.getCurPage().pid;
          return i;
        }
      }
      nextDirectoryPageId = dirPage.getNextPage();

      if (nextDirectoryPageId.pid != INVALID_PAGEID) {
        Minibase.BufferManager.unpinPage(dirPage.getCurPage(), UNPIN_CLEAN);
        Minibase.BufferManager.pinPage(nextDirectoryPageId, dirPage, PIN_DISKIO);
      }
    } while (nextDirectoryPageId.pid != INVALID_PAGEID);
    return -1;
    //throw new UnsupportedOperationException("Not implemented");

  } // protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)

  /**
   * Updates the directory entry for the given data page.
   * If the data page becomes empty, remove it.
   * If this causes a dir page to become empty, remove it
   * @param pageno identifies the data page whose directory entry will be updated
   * @param deltaRec input change in number of records on that data page
   * @param freecnt input new value of freecnt for the directory entry
   */
  protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {

    DirPage directoryPage = new DirPage();
    PageId directoryPageId = new PageId();
    int index = findDirEntry(pageno, directoryPageId, directoryPage);

    directoryPage.setRecCnt(index, (short) (directoryPage.getRecCnt(index) + deltaRec));
    directoryPage.setFreeCnt(index, (short) freecnt);
    Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_DIRTY);

    if (directoryPage.getRecCnt(index) < 1) {
      deletePage(pageno, directoryPageId, directoryPage, index);
    }
    //throw new UnsupportedOperationException("Not implemented");

  } // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

  /**
   * Inserts a new empty data page and its directory entry into the heap file.
   * If necessary, this also inserts a new directory page.
   * Leaves all data and directory pages unpinned
   *
   * @return id of the new data page
   */
  protected PageId insertPage() {

    DirPage directoryPage = new DirPage();
    PageId dataPageId = new PageId();
    PageId nextDirectoryPageId = new PageId();

    Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO);

    do {
      if (directoryPage.getEntryCnt() < directoryPage.getMaxEntries()) {

        dataPageId = Minibase.DiskManager.allocate_page();
        HFPage dataPage = new HFPage();
        dataPage.setCurPage(dataPageId);
        //create a page and add the directory to the correct slot and free up space
        directoryPage.setPageId(directoryPage.getEntryCnt(), dataPageId);
        directoryPage.setFreeCnt(directoryPage.getEntryCnt(), dataPage.getFreeSpace());
        directoryPage.setRecCnt(directoryPage.getEntryCnt(), (short) 0);
        directoryPage.setEntryCnt((short)(directoryPage.getEntryCnt() + 1));

        Minibase.BufferManager.pinPage(dataPageId, dataPage, PIN_MEMCPY);
        Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_DIRTY);
        Minibase.BufferManager.unpinPage(dataPage.getCurPage(), UNPIN_DIRTY);

        // Here we will return the PageId as after an insertion
        return dataPageId;
      }
      nextDirectoryPageId = directoryPage.getNextPage();

      if (nextDirectoryPageId.pid != INVALID_PAGEID) {

        Minibase.BufferManager.unpinPage(directoryPage.getCurPage(), UNPIN_CLEAN);
        Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_DISKIO);

      }
      else {
        // Creating the new page directory
        nextDirectoryPageId = Minibase.DiskManager.allocate_page();

        directoryPage.setNextPage(nextDirectoryPageId);
        PageId currentDirectoryPageId = directoryPage.getCurPage();
        Minibase.BufferManager.unpinPage(currentDirectoryPageId, UNPIN_DIRTY);
        //Remove the date from the current directory
        directoryPage = null;
        directoryPage = new DirPage();
        directoryPage.setCurPage(nextDirectoryPageId);
        directoryPage.setPrevPage(currentDirectoryPageId);

        Minibase.BufferManager.pinPage(nextDirectoryPageId, directoryPage, PIN_MEMCPY);
      }
    } while (nextDirectoryPageId.pid != INVALID_PAGEID);

    assert(false);
    dataPageId.pid = INVALID_PAGEID;
    return dataPageId;
    //throw new UnsupportedOperationException("Not implemented");

  } // protected PageId insertPage()

  /**
   * Deletes the given data page and its directory entry from the heap file. If
   * appropriate, this also deletes the directory page.
   *
   * @param pageno identifies the page to be deleted
   * @param dirId input param id of the directory page holding the data page's entry
   * @param dirPage input param to hold directory page contents
   * @param index input the data page's entry on the directory page
   */
  protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage, int index) {

    boolean clearDirectoryEntryFlag = false;
    //object for page ids that are invalid

    PageId invalidPageId = new PageId(INVALID_PAGEID);

    if (dirPage.getEntryCnt() < 2) {
      // Remove the references for the previous and next pages of directory
      if (dirPage.getPrevPage().pid != INVALID_PAGEID && dirPage.getNextPage().pid != INVALID_PAGEID) {

        DirPage previousPage = new DirPage();
        DirPage nextPage = new DirPage();
        Minibase.BufferManager.pinPage(dirPage.getPrevPage(), previousPage, PIN_DISKIO);
        Minibase.BufferManager.pinPage(dirPage.getNextPage(), nextPage, PIN_DISKIO);

        previousPage.setNextPage(nextPage.getCurPage());
        nextPage.setPrevPage(previousPage.getCurPage());

        Minibase.BufferManager.unpinPage(nextPage.getCurPage(), UNPIN_DIRTY);
        Minibase.BufferManager.unpinPage(previousPage.getCurPage(), UNPIN_DIRTY);

        Minibase.BufferManager.freePage(dirId);
      }

      else if (dirPage.getPrevPage().pid != INVALID_PAGEID) {

        DirPage previousPage = new DirPage();
        Minibase.BufferManager.pinPage(dirPage.getPrevPage(), previousPage, PIN_DISKIO);

        previousPage.setNextPage(invalidPageId);
        Minibase.BufferManager.unpinPage(dirPage.getPrevPage(), UNPIN_DIRTY);
        Minibase.BufferManager.freePage(dirId);
      }
      else if (INVALID_PAGEID != dirPage.getNextPage().pid) {
        DirPage nextPage = new DirPage();
        Minibase.BufferManager.pinPage(dirPage.getNextPage(), nextPage, PIN_DISKIO);

        nextPage.setPrevPage(invalidPageId);
        Minibase.BufferManager.unpinPage(dirPage.getNextPage(), UNPIN_DIRTY);
        Minibase.BufferManager.freePage(dirId);
      }
      else {
        clearDirectoryEntryFlag = true;
      }
    }
    else
    {
      clearDirectoryEntryFlag = true;
    }

    if (clearDirectoryEntryFlag) {

      dirPage.setPageId(index, invalidPageId);
      dirPage.setRecCnt(index, (short) 0);
      dirPage.setFreeCnt(index, (short) 0);
      dirPage.compact(index);

      Minibase.BufferManager.pinPage(dirId, dirPage, PIN_MEMCPY);
      Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
    }

    Minibase.BufferManager.freePage(pageno);
    //throw new UnsupportedOperationException("Not implemented");

  } // protected void deletePage(PageId, PageId, DirPage, int)

  /**
   * Creates an empty heapfile.  Used by HeapFile constructor.
   */
  protected void createNewHF() {

    // Adding a new entry for a heap file
    DirPage mainDirPage = new DirPage();
    headId = Minibase.DiskManager.allocate_page();
    Minibase.DiskManager.add_file_entry(fileName, headId);

    mainDirPage.setCurPage(headId);
    Minibase.BufferManager.pinPage(headId, mainDirPage, PIN_MEMCPY);
    Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);

  } // protected void CreateEmptyHeapFile()

} // public class HeapFile implements GlobalConst
