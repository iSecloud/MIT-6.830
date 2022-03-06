package simpledb.transaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hamcrest.core.IsInstanceOf;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {
	
	/**
	 * LOCK: 全局锁，用于保证获取/释放读写锁操作的一致性
	 * tIdToLocksMap： 事务t锁持有的锁集合
	 * pageToLockMap： 每个page对应的锁(每个page都是有自己独一无二的锁)
	 * tIdWatiToGetWriteLock：等待写锁的事务集合
	 * 	 这个数据结构是为了保证前面事务wait的写锁能及时获得，用于阻塞后面事务获取读锁，否则会出现脏读的情况
	 *   举个例子: 下面是获取锁的顺序：
	 *   r1 r2 w1 w2 r3 w3
	 *   首先r1 r2获得读锁没问题，事务1获取w锁即w1时没有出现死锁情况，这时候会进行锁升级将r1释放掉，w1阻塞，
	 *   然后尝试获取w2时会发现出现了死锁情况，事务2回滚导致r2被释放掉同时唤醒等待的写锁w1。
	 *   下面有一个究极扯淡的地方就是如果好巧不巧的是正好r3得到了读锁，那么w2继续阻塞
	 *   然后尝试获取w3，这个时候是能获取成功的，因为r1被释放了，r2回滚了，r3进行了锁升级，所以此时不存在读锁。
	 *   获取以后事务3进行各种写入，此时的page已经成为了脏页，事务3结束以后唤醒了w1，
	 *   问题来了，r1获得的数据是w3更新前的数据，故出现了脏读的情况。
	 *   解决方案:
	 *   如果一个事务t获取w锁阻塞了，则阻塞后面所有事务获取读/写锁，直到该事务获得了w锁并事务结束/回滚
	 * deadlockManager：死锁管理器
	 */
	public static final Object LOCK = new Object();
	public ConcurrentHashMap<TransactionId, CopyOnWriteArraySet<RWLock>> tIdToLocksMap;
	public ConcurrentHashMap<PageId, RWLock> pageToLockMap;
	public ConcurrentHashMap<PageId, Permissions> pageToPermMap;
	public ConcurrentHashMap<PageId, CopyOnWriteArraySet<TransactionId>> tIdWatiToGetPageWriteLockMap;
	public DeadlockManager deadlockManager;
	
	public LockManager() {
		tIdToLocksMap = new ConcurrentHashMap<TransactionId, CopyOnWriteArraySet<RWLock>>();
		pageToLockMap = new ConcurrentHashMap<PageId, RWLock>();
		pageToPermMap = new ConcurrentHashMap<PageId, Permissions>();
		deadlockManager = new DeadlockManager();
		tIdWatiToGetPageWriteLockMap = new ConcurrentHashMap<PageId, CopyOnWriteArraySet<TransactionId>>();
	}
	
	public void lock(TransactionId tId, PageId pageId, Permissions perm) throws TransactionAbortedException {
		synchronized (LockManager.LOCK) {
			// String lockType = perm.equals(Permissions.READ_ONLY) ? "read_only" : "read_write";
			// System.out.printf("I am Thread%s, I am trying to get %s\n", Thread.currentThread().getName(), lockType);
			
			// 如果该页面未曾加锁，则初始化一个页级锁
			if (!pageToLockMap.containsKey(pageId)) {
				RWLock rwLock = new RWLock();
				pageToLockMap.put(pageId, rwLock);
			}
			// 如果该事务未曾持有锁集合，则初始化一个锁集合
			if (!tIdToLocksMap.containsKey(tId)) {
				CopyOnWriteArraySet<RWLock> lockSet = new CopyOnWriteArraySet<RWLock>();
	    		tIdToLocksMap.put(tId, lockSet);
	    	}
			// 检测是否有死锁存在
			deadlockManager.deadlockDetected(tId, pageToLockMap.get(pageId), perm);
			// System.out.printf("I am %s, There is no deadlock\n", Thread.currentThread().getName());
			try {
				pageToLockMap.get(pageId).lock(this, tId, pageId, perm);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void unlock(TransactionId tId, PageId pId) {
		synchronized (LockManager.LOCK) {
			if (!pageToLockMap.containsKey(pId)) {
	        	return;
	        } 
	        RWLock rwLock = pageToLockMap.get(pId);
	        if (!tIdToLocksMap.getOrDefault(tId, new CopyOnWriteArraySet<RWLock>()).contains(rwLock)) {
	        	return;
	        }
			if (pageToPermMap.get(pId).equals(Permissions.READ_ONLY)) {
				//System.out.printf("\nI am Thread%s, read lock:%d write lock:%d\n", Thread.currentThread().getName(), rwLock.getReadLockNum(), rwLock.getWriteLockNum());
				rwLock.readUnlock(this);
				if (rwLock.getReadLockNum() == 0 && pageToPermMap.get(pId).equals(Permissions.READ_ONLY)) {
					pageToPermMap.remove(pId);
				}
				//System.out.printf("I am Thread%s, read lock:%d write lock:%d\n", Thread.currentThread().getName(), rwLock.getReadLockNum(), rwLock.getWriteLockNum());
	    	} else {
	    		//System.out.printf("\nI am Thread%s, read lock:%d write lock:%d\n", Thread.currentThread().getName(), rwLock.getReadLockNum(), rwLock.getWriteLockNum());
	    		rwLock.writeUnlock(this);
	    		if (rwLock.getWriteLockNum() == 0 && pageToPermMap.get(pId).equals(Permissions.READ_WRITE)) {
					pageToPermMap.remove(pId);
				}
	    		//System.out.printf("I am Thread%s, read lock:%d write lock:%d\n", Thread.currentThread().getName(), rwLock.getReadLockNum(), rwLock.getWriteLockNum());
	    	}
			//printInfo();
			deadlockManager.releaseTransLock(tId, rwLock);
			tIdToLocksMap.get(tId).remove(pageToLockMap.get(pId));
		}
	}
	
	public boolean holdsLock(TransactionId tid, PageId pid) {
    	if (!pageToLockMap.containsKey(pid)) {
        	return false;
        } 
        RWLock lock = pageToLockMap.get(pid);
        if (!tIdToLocksMap.get(tid).contains(lock)) {
        	return false;
        }
        return true;
    }
	
	public void printInfo() {
	    for (PageId pageId: pageToLockMap.keySet()) {
	    	System.out.printf("This page is %d\n", pageId.getPageNumber());
	    	System.out.printf("read lock:%d write lock:%d\n", pageToLockMap.get(pageId).getReadLockNum(), pageToLockMap.get(pageId).getWriteLockNum());
	    	if (pageToPermMap.containsKey(pageId)) {
	    		String perm = pageToPermMap.get(pageId).equals(Permissions.READ_ONLY) ? "read_only" : "read_write";
		    	System.out.printf("The permission is %s\n", perm);
	    	}
	    }
	}
	
//	public void releaseTransWaitLock(TransactionId tId) {
//		deadlockManager.releaseTransLock(tId, tIdToLocksMap);
//	}
}
