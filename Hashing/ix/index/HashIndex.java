package index;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SearchKey;
import global.Minibase;

/**
 * <h3>Minibase Hash Index</h3>
 * This unclustered index implements static hashing as described on pages 371 to
 * 373 of the textbook (3rd edition).  The index file is a stored as a heapfile.  
 */
public class HashIndex implements GlobalConst {

  /** File name of the hash index. */
  protected String fileName;

  /** Page id of the directory. */
  protected PageId headId;
  
  //Log2 of the number of buckets - fixed for this simple index
  protected final int  DEPTH = 7;

  // --------------------------------------------------------------------------

  /**
   * Opens an index file given its name, or creates a new index file if the name
   * doesn't exist; a null name produces a temporary index file which requires
   * no file library entry and whose pages are freed when there are no more
   * references to it.
   * The file's directory contains the locations of the 128 primary bucket pages.
   * You will need to decide on a structure for the directory.
   * The library entry contains the name of the index file and the pageId of the
   * file's directory.
   */
  public HashIndex(String fileName) {

      this.fileName = fileName;
      boolean exists = false;
      if(fileName!= null){
          this.headId = Minibase.DiskManager .get_file_entry(fileName);
          if(this.headId != null){
              exists=true;
          }
      }

      if(!exists){
          HashDirPage dirPage = new HashDirPage();
          this.headId = Minibase.BufferManager.newPage(dirPage, 1);
          Minibase.BufferManager.unpinPage(this.headId, UNPIN_DIRTY);

          if(fileName!=null){
              Minibase.DiskManager.add_file_entry(fileName, this.headId);
          }
      }
	  //throw new UnsupportedOperationException("Not implemented");

  } // public HashIndex(String fileName)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the index file if it's temporary.
   */
  protected void finalize() throws Throwable {

      if(this.fileName==null) deleteFile();
      //throw new UnsupportedOperationException("Not implemented");

  } // protected void finalize() throws Throwable

   /**
   * Deletes the index file from the database, freeing all of its pages.
   */
  public void deleteFile() {

      PageId dirId = new PageId(this.headId.pid);
      HashDirPage dirPage = new HashDirPage();
      HashBucketPage dataPage = new HashBucketPage();
      while (dirId.pid!=INVALID_PAGEID){
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          int count = dirPage.getEntryCount();

          for (int i=0; i<count; ++i){
              PageId dataId = dirPage.getPageId(i);
              while(dataId.pid!=INVALID_PAGEID){
                  Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
                  PageId nextId=dataPage.getNextPage();
                  Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
                  Minibase.BufferManager.freePage(dataId);
                  dataId = nextId;
              }
          }

          PageId nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          Minibase.BufferManager .freePage(dirId);
          dirId = nextId;
      }

      if (this.fileName!=null){
          Minibase.DiskManager.delete_file_entry(this.fileName);
      }
	  //throw new UnsupportedOperationException("Not implemented");

  } // public void deleteFile()

  /**
   * Inserts a new data entry into the index file.
   * 
   * @throws IllegalArgumentException if the entry is too large
   */
  public void insertEntry(SearchKey key, RID rid) {

      DataEntry entry = new DataEntry (key, rid);
      if (entry.getLength()>1012){
          throw new IllegalArgumentException("entry too large");
      }

      int hash = key.getHash(this.DEPTH);

      PageId dirId = new PageId(this.headId.pid);
      HashDirPage dirPage =new HashDirPage();
      while (hash >= HashDirPage.MAX_ENTRIES){
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          PageId nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          dirId = nextId;
          hash-= HashDirPage.MAX_ENTRIES;
      }
      Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO); //bigmistake corrected!

      PageId dataId=dirPage.getPageId(hash);
      HashBucketPage dataPage = new HashBucketPage();
      if (dataId.pid!= INVALID_PAGEID){
          Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);

      }else {
          dataId=Minibase.BufferManager.newPage(dataPage, 1);
          dirPage.setPageId(hash, dataId);
          Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
      }

      boolean dirty =dataPage.insertEntry(entry);
      Minibase.BufferManager.unpinPage(dataId, dirty);
	  //throw new UnsupportedOperationException("Not implemented");

  } // public void insertEntry(SearchKey key, RID rid)

  /**
   * Deletes the specified data entry from the index file.
   * 
   * @throws IllegalArgumentException if the entry doesn't exist
   */
  public void deleteEntry(SearchKey key, RID rid) {

      int hash = key.getHash(this.DEPTH);
      DataEntry entry = new DataEntry(key, rid);

      PageId dirId= new PageId(this.headId.pid);
      HashDirPage dirPage = new HashDirPage();
      while (hash>=HashDirPage.MAX_ENTRIES){
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          PageId nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          dirId = nextId;
          hash -= HashDirPage.MAX_ENTRIES;
      }
      Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);

      PageId dataId =dirPage.getPageId(hash);
      Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
      HashBucketPage dataPage = new HashBucketPage();
      if (dataId.pid!=INVALID_PAGEID){
          Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
      }else{
          throw new IllegalArgumentException("entry doesn't exist");
      }

      try {
          boolean dirty = dataPage.deleteEntry(entry);
          Minibase.BufferManager.unpinPage(dataId, dirty);
      } catch(IllegalArgumentException exc){
          Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
          throw exc;
      }
      //throw new UnsupportedOperationException("Not implemented");

  } // public void deleteEntry(SearchKey key, RID rid)

  /**
   * Initiates an equality scan of the index file.
   */
  public HashScan openScan(SearchKey key) {
    return new HashScan(this, key);
  }

  /**
   * Returns the name of the index file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Prints a high-level view of the directory, namely which buckets are
   * allocated and how many entries are stored in each one. Sample output:
   * 
   * <pre>
   * IX_Customers
   * ------------
   * 0000000 : 35
   * 0000001 : null
   * 0000010 : 27
   * ...
   * 1111111 : 42
   * ------------
   * Total : 1500
   * </pre>
   */
  public void printSummary() {

      System.out.println();
      String name = (this.fileName!= null)? this.fileName:"(temp)";
      System.out.println(name);

      for (int j=0; j<name.length(); ++j){
          System.out.print("-");
      }
      System.out.println();
      int total =0;

      PageId dirId = new PageId(this.headId.pid);
      HashDirPage dirPage = new HashDirPage();
      HashBucketPage dataPage = new HashBucketPage();
      while (dirId.pid !=INVALID_PAGEID){
          Minibase.BufferManager.pinPage(dirId, dirPage, PIN_DISKIO);
          int count = dirPage.getEntryCount();

          for (int i=0; i<count;++i){
              String hash = Integer.toString(i,2);
              for (int j=0; j<this.DEPTH-hash.length(); ++j){
                  System.out.print('0');
              }
              System.out.print(hash+" : ");

              PageId dataId = dirPage.getPageId(i);
              if (dataId.pid != INVALID_PAGEID){
                  Minibase.BufferManager.pinPage(dataId, dataPage, PIN_DISKIO);
                  int bkcnt = dataPage.countEntries();
                  System.out.println(bkcnt);
                  total += bkcnt;
                  Minibase.BufferManager.unpinPage(dataId, UNPIN_CLEAN);
              } else {
                  System.out.println("null");
              }
          }// end forloop

          PageId nextId = dirPage.getNextPage();
          Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
          dirId = nextId;

      }// end while loop

      for (int j=0; j<name.length();++j){
          System.out.print('-');
      }
      System.out.println();
      System.out.println("Total : "+ total);
	  //throw new UnsupportedOperationException("Not implemented");

  } // public void printSummary()

} // public class HashIndex implements GlobalConst
