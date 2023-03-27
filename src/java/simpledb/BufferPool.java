package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    public static int MANUAL_PAGES;

    LockManager lockManager;
    /**
     * key : hash code of PageId
     * value : Page file'
     */
    // TODO: Convert key from pageId.hashCode() to pageId
    public final HashMap<Integer, Page> bufferPool;
    public int pageNum;
    /**
     * Implement least-recent-used policy to this BufferPool
     * Top of this queue is the least recent used pageId.hashCode
     */
    private final Queue<Integer> LRUCache;
    // Record the page num within this buffer pool
    private final Object LOCK;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        MANUAL_PAGES = numPages;
        pageNum = 0;
        bufferPool = new HashMap<>();
        LRUCache = new LinkedList<>();
        lockManager = new LockManager();
        LOCK = new Object();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        if (perm.toString().equals("UNKNOWN"))
            throw new DbException("No permission");

        synchronized (LOCK) {
            // Requesting a lock ...
            try {
                while (!lockManager.acquireLock(tid, pid, perm)) LOCK.wait();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }

            if (bufferPool.containsKey(pid.hashCode())) {
                makeRecentUsed(pid.hashCode());
                return bufferPool.get(pid.hashCode());
            }

            while (this.pageNum >= MANUAL_PAGES) {
                evictPage();
            }

            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page requiredPage = dbFile.readPage(pid);
            bufferPool.put(pid.hashCode(), requiredPage);
            makeRecentUsed(pid.hashCode());
            this.pageNum++;

            LOCK.notifyAll();
            return requiredPage;
        }
    }

    /**
     * Helper function to make a page "Most recent used" in LRUCache queue
     * @param pidKey
     *          HashCode of PageID
     */
    private void makeRecentUsed(int pidKey) {
        if (LRUCache.contains(pidKey)) {
            LRUCache.remove(pidKey);
            LRUCache.offer(pidKey);
        } else {
            LRUCache.offer(pidKey);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // TODO: some code goes here
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // TODO: some code goes here
        synchronized (LOCK) {
            Set<PageId> pageIdSet = lockManager.tidToPages.get(tid);
            if (pageIdSet == null) {
                lockManager.releaseTransaction(tid);
                LOCK.notifyAll();
                return;
            }
            for (PageId pageId : pageIdSet) {
                if (!bufferPool.containsKey(pageId.hashCode())) continue;
                Page page = bufferPool.get(pageId.hashCode());
                if (commit) {
                    // Commit the modifications: FORCE
                    flushPage(pageId);
                    Page p = bufferPool.get(pageId.hashCode());
                    p.setBeforeImage();
                } else {
                    // Abort the modifications: NO-STEAL
                    // Overwrite the page in BufferPool with the old version
                    Page oldPage = page.getBeforeImage();
                    bufferPool.put(oldPage.getId().hashCode(), oldPage);
                }
            }
            lockManager.releaseTransaction(tid);
            LOCK.notifyAll();
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile affectedDbFile = Database.getCatalog().getDatabaseFile(tableId);
        affectedDbFile.insertTuple(tid, t);
        // TODO: NO-STEAL: We never evict a dirty page
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        PageId pid = t.recordId.getPageId();
        DbFile affectedDbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        affectedDbFile.deleteTuple(tid, t);
        // NO-STEAL: We never evict a dirty page
        LRUCache.remove(pid.hashCode());
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p : bufferPool.values()) {
            flushPage(p.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        LRUCache.remove(pid.hashCode());
        bufferPool.remove(pid.hashCode());
        this.pageNum--;
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        HeapPage pageNeedFlush = (HeapPage) bufferPool.get(pid.hashCode());
        TransactionId dirtier = pageNeedFlush.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, pageNeedFlush.getBeforeImage(), pageNeedFlush);
            Database.getLogFile().force();
            DbFile flushDbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            flushDbFile.writePage(pageNeedFlush);
            pageNeedFlush.markDirty(false, null);
            makeRecentUsed(pid.hashCode());
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        for (Page p : bufferPool.values()) {
            if (tid.equals(p.isDirty())) flushPage(p.getId());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        if (LRUCache.size() == 0) throw new DbException("Empty LRU Cache");

        int evictedPidKey = LRUCache.peek();
        HeapPage evictedPage = (HeapPage) bufferPool.get(evictedPidKey);

        // NO-STEAL: Never flush dirty page into disk
        if (evictedPage.isDirty() == null) discardPage(evictedPage.pid);
        else LRUCache.poll();
    }

}
