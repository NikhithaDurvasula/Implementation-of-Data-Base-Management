package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3> The buffer manager manages an array of main
 * memory pages. The array is called the buffer pool, each page is called a
 * frame. It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and relational
 * operators.
 */
public class BufMgr implements GlobalConst {

    public FrameDesc[] buffpool;
    public HashMap<PageId, FrameDesc> buffmap;
    public Clock replPolicy;

    /**
     * Constructs a buffer manager by initializing member data.
     *
     * @param numframes
     *            number of frames in the buffer pool
     */
    public BufMgr(int numframes) {
        buffmap = new HashMap<PageId, FrameDesc>();
        buffpool = new FrameDesc[numframes];
        for (int i = 0; i < numframes; i++) {
            buffpool[i] = new FrameDesc();
        }
        replPolicy = new Clock(buffpool);
        // throw new UnsupportedOperationException("Not implemented");
    } // public BufMgr(int numframes)

    /**
     * The result of this call is that disk page number pageno should reside in
     * a frame in the buffer pool and have an additional pin assigned to it, and
     * mempage should refer to the contents of that frame. <br>
     * <br>
     *
     * If disk page pageno is already in the buffer pool, this simply increments
     * the pin count. Otherwise, this<br>
     *
     * <pre>
     * 	uses the replacement policy to select a frame to replace
     * 	writes the frame's contents to disk if valid and dirty
     * 	if (contents == PIN_DISKIO)
     * 		read disk page pageno into chosen frame
     * 	else (contents == PIN_MEMCPY)
     * 		copy mempage into chosen frame
     * 	[omitted from the above is maintenance of the frame table and hash map]
     * </pre>
     *
     * @param pageno
     *            identifies the page to pin
     * @param mempage
     *            An output parameter referring to the chosen frame. If
     *            contents==PIN_MEMCPY it is also an input parameter which is
     *            copied into the chosen frame, see the contents parameter.
     * @param contents
     *            Describes how the contents of the frame are determined.<br>
     *            If PIN_DISKIO, read the page from disk into the frame.<br>
     *            If PIN_MEMCPY, copy mempage into the frame.<br>
     *            If PIN_NOOP, copy nothing into the frame - the frame contents
     *            are irrelevant.<br>
     *            Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is
     *            avoided.
     * @throws IllegalArgumentException
     *             if PIN_MEMCPY and the page is pinned.
     * @throws IllegalStateException
     *             if all pages are pinned (i.e. pool is full)
     */
    public void pinPage(PageId pageno, Page mempage, int contents) {
        //uses the "Clock" replacement policy to select a frame to replace
        FrameDesc victimFrame = null;
        if (buffmap.containsKey(pageno)) {
            victimFrame = buffmap.get(pageno);
            if (victimFrame != null) {
                victimFrame.pinCount++;
            }
        }
        if (!buffmap.containsKey(pageno)) {

            int victim = replPolicy.pickVictim();
            if (victim == -1) {
                throw new IllegalStateException("The pages in bufferpool are pinned");
            }

            victimFrame = buffpool[victim];

            // if victim frame is dirty && valid, flush it to disk
            if (victimFrame.dirtyBit == true && victimFrame.validBit == true) {
                Minibase.DiskManager.write_page(victimFrame.pageId(), victimFrame);
            }

            // Read page from disk into the victim frame
            if (contents == PIN_DISKIO) {
                Minibase.DiskManager.read_page(pageno, victimFrame);
            }
            // mempage is copied into the victim frame
            else if (contents == PIN_MEMCPY) {
                victimFrame.copyPage(mempage);
            }

            //remove previous page from the frame if any
            buffmap.remove(victimFrame.pageId());

            //reset the frame details
            victimFrame.pageNum.copyPageId(pageno);
            victimFrame.dirtyBit = false;
            victimFrame.validBit = true;
            victimFrame.referenceBit = true;
            victimFrame.pinCount = 1;
            buffmap.put(victimFrame.pageId(), victimFrame);
        }
        mempage.setPage(victimFrame);

        // throw new UnsupportedOperationException("Not implemented");
    } // public void pinPage(PageId pageno, Page page, int contents)

    /**
     * Unpins a disk page from the buffer pool, decreasing its pin count.
     *
     * @param pageno
     *            identifies the page to unpin
     * @param dirty
     *            UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
     * @throws IllegalArgumentException
     *             if the page is not in the buffer pool or not pinned
     */
    public void unpinPage(PageId pageno, boolean dirty) {
        FrameDesc frameNum = null;

        if (!buffmap.containsKey(pageno)) {
            frameNum = null;
            throw new IllegalArgumentException(
                    "there is no such page in bufferpool");
        }
        if (buffmap.containsKey(pageno)) {
            frameNum = buffmap.get(pageno);
            if (!(frameNum.pinCount > 0)) {
                throw new IllegalArgumentException("Page is not pinned");
            }

            // Unpin page
            frameNum.pinCount = frameNum.pinCount-1;
            frameNum.setDirtyBit(true);
            ;
        }
    }

    // throw new UnsupportedOperationException("Not implemented");

    // public void unpinPage(PageId pageno, boolean dirty)

    /**
     * Allocates a run of new disk pages and pins the first one in the buffer
     * pool. The pin will be made using PIN_MEMCPY. Watch out for disk page
     * leaks.
     *
     * @param firstpg
     *            input and output: holds the contents of the first allocated
     *            page and refers to the frame where it resides
     * @param run_size
     *            input: number of pages to allocate
     * @return page id of the first allocated page
     * @throws IllegalArgumentException
     *             if firstpg is already pinned
     * @throws IllegalStateException
     *             if all pages are pinned (i.e. pool exceeded)
     */
    public PageId newPage(Page firstpg, int run_size) {
        PageId pageNum = Minibase.DiskManager.allocate_page(run_size);

        if (!buffmap.containsKey(firstpg)) {
            try {
                pinPage(pageNum, firstpg, PIN_MEMCPY);
            } catch (IllegalArgumentException exception) {
                Minibase.DiskManager.deallocate_page(pageNum, run_size);
                throw new IllegalArgumentException("Page is already pinned");
            } catch (IllegalStateException e) {
                Minibase.DiskManager.deallocate_page(pageNum, run_size);
                throw new IllegalStateException("All pages are pinned");
            }
        }
        return pageNum;
        // throw new UnsupportedOperationException("Not implemented");

    } // public PageId newPage(Page firstpg, int run_size)

    /**
     * Deallocates a single page from disk, freeing it from the pool if needed.
     *
     * @param pageno
     *            identifies the page to remove
     * @throws IllegalArgumentException
     *             if the page is pinned
     */
    public void freePage(PageId pageno) {
        FrameDesc frameNum = null;
        if (!buffmap.containsKey(pageno)) {
            return;
        }
        if (buffmap.containsKey(pageno)) {
            frameNum = buffmap.get(pageno);
            if (frameNum.pinCount > 0) {
                throw new IllegalArgumentException("Page is pinned");
            }
            // The slot in bufferPool will be overwritten, just need to track in
            // frameTable
            Minibase.DiskManager.deallocate_page(frameNum.pageId());
            buffmap.remove(frameNum.pageId());
            frameNum.setValidBit(false);
        }

    } // public void freePage(PageId firstid)

    /**
     * Write all valid and dirty frames to disk. Note flushing involves only
     * writing, not unpinning or freeing or the like.
     *
     */
    public void flushAllFrames() {
        for (PageId p : buffmap.keySet()) {
            flushPage(p);
        }

    } // public void flushAllFrames()

    /**
     * Write a page in the buffer pool to disk, if dirty.
     *
     * @throws IllegalArgumentException
     *             if the page is not in the buffer pool
     */
    public void flushPage(PageId pageno) {
        FrameDesc frameNum = null;

        if (!buffmap.containsKey(pageno)) {
            throw new IllegalArgumentException("Page is not in the buffer pool");
        }
        if (buffmap.containsKey(pageno)) {
            frameNum = buffmap.get(pageno);
            if (frameNum.dirtyBit = true && frameNum.validBit == true) {
                //write page to disk
                Minibase.DiskManager.write_page(frameNum.pageId(), frameNum);
                frameNum.setDirtyBit(false);
            }
        }
    }

    /**
     * Gets the total number of buffer frames.
     */
    public int getNumFrames() {
        return buffpool.length;
    }

    /**
     * Gets the total number of unpinned buffer frames.
     */
    public int getNumUnpinned() {
        int total = 0;
        for (FrameDesc f : buffpool) {
            if (!(f.pinCount > 0)) {
                total++;
            }
        }
        return total;
    }
} // public class BufMgr implements GlobalConst
