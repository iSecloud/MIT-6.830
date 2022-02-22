package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.DeadlockManager;
import simpledb.transaction.LockManager;
import simpledb.transaction.RWLock;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    
    private int numPages;
    private ConcurrentHashMap<Integer, Page> pagesMap;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pagesMap = new ConcurrentHashMap<Integer, Page>(numPages);
        lockManager = new LockManager();
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
    	lockManager.lock(tid, pid, perm);
    	Integer pageHashCode = pid.hashCode();
    	if (!pagesMap.containsKey(pageHashCode)) {
        	// 实例化一个Dbfile: 从Catalog得到（通过全局Dtabase得到Catalog实例对象）
    		DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page page = dbFile.readPage(pid);
    		if (pagesMap.size() >= numPages) {
    			evictPage();
    		}
    		pagesMap.put(pid.hashCode(), page);
        }
        return pagesMap.get(pid.hashCode());
    }
    
    public LockManager getLockManager() {
    	return lockManager;
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
    	return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // 先对修改得页进行操作，然后释放对应得锁
    	try {
    		if (commit) {
    			flushPages(tid);
    		} else {
    			restorePages(tid);
    		}
    	} catch (IOException e) {
			e.printStackTrace();
		}
    	// lockManager.releaseTransWaitLock(tid);
    	Iterator<Map.Entry<Integer, Page>> pageIterator = pagesMap.entrySet().iterator();
    	while (pageIterator.hasNext()) {
    		Page page = pageIterator.next().getValue();
    		unsafeReleasePage(tid, page.getId());
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
    public void insertTuple(TransactionId tid, int tableId, Tuple tup)
        throws DbException, IOException, TransactionAbortedException {
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = heapFile.insertTuple(tid, tup);
        for (Page page: pageList) {
        	HeapPage heapPage = (HeapPage) page;
        	heapPage.markDirty(true, tid);
        	pagesMap.put(heapPage.getId().hashCode(), heapPage);
        }
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
    public void deleteTuple(TransactionId tid, Tuple tup)
        throws DbException, IOException, TransactionAbortedException {
    	 HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tup.getRecordId().getPageId().getTableId());
         List<Page> pageList = heapFile.deleteTuple(tid, tup);
         for (Page page: pageList) {
         	HeapPage heapPage = (HeapPage) page;
         	heapPage.markDirty(true, tid);
         	pagesMap.put(heapPage.getId().hashCode(), heapPage);
         }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        for (Map.Entry<Integer, Page> entry: pagesMap.entrySet()) {
        	HeapPage page = (HeapPage) entry.getValue();
        	if (page.isDirty() != null) {
        		flushPage(page.getId());
        	}
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public void discardPage(PageId pid) {
        pagesMap.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        Page page = pagesMap.get(pid.hashCode());
        TransactionId tid = page.isDirty(); // 获取脏页的事务，如果为null说明不是脏页
        if (tid != null) {
        	// TODO 日志记录，便于回滚 Database.getLogFile().logWrite
        	HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        	heapFile.writePage(page);
        	page.markDirty(false, null);
        }
    }
    
    private void restorePage(PageId pid) throws IOException {
    	Page page = pagesMap.get(pid.hashCode());
    	TransactionId tid = page.isDirty();
    	if (tid != null) {
    		page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    		pagesMap.put(page.getId().hashCode(), page);
    		page.markDirty(false, null);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (RWLock rwLock: lockManager.tIdToLocksMap.get(tid)) {
        	for (PageId pageId: lockManager.pageToLockMap.keySet()) {
        		if (rwLock.equals(lockManager.pageToLockMap.get(pageId)) 
        				&& pagesMap.containsKey(pageId.hashCode())) {
        			flushPage(pageId);
        			break;
        		}
        	}
        }
    }
    
    public synchronized void restorePages(TransactionId tid) throws IOException {
    	for (RWLock rwLock: lockManager.tIdToLocksMap.get(tid)) {
        	for (PageId pageId: lockManager.pageToLockMap.keySet()) {
        		if (rwLock.equals(lockManager.pageToLockMap.get(pageId)) 
        				&& pagesMap.containsKey(pageId.hashCode())) {
        			restorePage(pageId);	
        			break;
        		}
        	}
        } 
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        Iterator<Map.Entry<Integer, Page>> pageIterator = pagesMap.entrySet().iterator();
        // 不能驱逐脏页，事务的修改只有在提交后才会写入磁盘 —— no steal 策略
        while (pageIterator.hasNext()) {
        	Map.Entry<Integer, Page> item = pageIterator.next();
        	HeapPage page = (HeapPage) item.getValue();
        	if (page.isDirty() == null) {
        		try {
					flushPage(page.getId());
					discardPage(page.getId());
					return;
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        }
        // 没有一个非脏页可以用来驱逐，则抛出异常
        throw new DbException("No non-dirty pages can be used for expulsion!");
    }

}
