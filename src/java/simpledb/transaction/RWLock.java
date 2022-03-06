package simpledb.transaction;

import java.util.concurrent.CopyOnWriteArraySet;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class RWLock {
	private int readLockCount;
	private int writeLockCount;
	private final int RESET = 5 * 1000;
	
	public void RWLock() {
		readLockCount = 0;
		writeLockCount = 0;
	}
    
    public void lock(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
    	// 如果当前有读锁且想获得写锁，则进行锁升级
    	if (isLockUpgrade(lockManager, tId, pId, perm)) {
    		lockUpgrade(lockManager, tId, pId, perm);
    	}
    	// 如果重复获得同一个锁/低级锁，则跳过
    	else if (isLockReapet(lockManager, tId, pId, perm)) {
    		return;
    	}
    	else if (perm.equals(Permissions.READ_ONLY)) {
    		readLock(lockManager, tId, pId, perm);
    	} 
    	else {
    		writeLock(lockManager, tId, pId, perm);
    	}
    }
    
    public boolean isLockUpgrade(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) {
    	if (lockManager.tIdToLocksMap.get(tId) == null || lockManager.pageToPermMap.get(pId) == null) {
    		return false;
    	}
    	if (!lockManager.tIdToLocksMap.get(tId).contains(lockManager.pageToLockMap.get(pId))) {
    		return false;
    	}
    	if (lockManager.pageToPermMap.get(pId).equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_WRITE)) {
    		return true;
    	}
    	return false;
    }
    
    public boolean isLockReapet(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) {
    	if (lockManager.tIdToLocksMap.get(tId) == null || lockManager.pageToPermMap.get(pId) == null) {
    		return false;
    	}
    	if (!lockManager.tIdToLocksMap.get(tId).contains(lockManager.pageToLockMap.get(pId))) {
    		return false;
    	}
    	if (lockManager.pageToPermMap.get(pId).equals(Permissions.READ_WRITE) 
    			|| lockManager.pageToPermMap.get(pId).equals(perm)) {
    		return true;
    	}
    	return false;
    }
    
    public void writeLock(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
    	synchronized (LockManager.LOCK) {
    		if (lockManager.tIdWatiToGetPageWriteLockMap.get(pId) == null) {
				lockManager.tIdWatiToGetPageWriteLockMap.put(pId, new CopyOnWriteArraySet<TransactionId>());
			}
    		while (writeLockCount + readLockCount != 0 
    				|| lockManager.tIdWatiToGetPageWriteLockMap.get(pId).size() > 1
    				|| (lockManager.tIdWatiToGetPageWriteLockMap.get(pId).size() == 1 
    					&& !lockManager.tIdWatiToGetPageWriteLockMap.get(pId).contains(tId))) {
    			// TODO lockManager.tIdWatiToGetWriteLock.size() > 1 这个条件可否不要？
    			// 因为逻辑上只能先获取读锁再获取写锁
    			if (lockManager.tIdWatiToGetPageWriteLockMap.get(pId).size() == 0) {
    				lockManager.tIdWatiToGetPageWriteLockMap.get(pId).add(tId);
    			}  
    			LockManager.LOCK.wait();
        	}
    		if (lockManager.tIdWatiToGetPageWriteLockMap.get(pId).contains(tId)) {
    			lockManager.tIdWatiToGetPageWriteLockMap.get(pId).remove(tId);
    		}
        	writeLockCount += 1;
        	lockManager.tIdToLocksMap.get(tId).add(lockManager.pageToLockMap.get(pId));
    		lockManager.pageToPermMap.put(pId, perm);
    		// System.out.printf("I am %s I got the write lock!! read lock:%d write lock:%d\n", Thread.currentThread().getName(), readLockCount, writeLockCount);
		}
    }
    
    public void readLock(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
    	synchronized (LockManager.LOCK) {
    		if (lockManager.tIdWatiToGetPageWriteLockMap.get(pId) == null) {
				lockManager.tIdWatiToGetPageWriteLockMap.put(pId, new CopyOnWriteArraySet<TransactionId>());
			}
    		while (writeLockCount != 0 || lockManager.tIdWatiToGetPageWriteLockMap.get(pId).size() != 0) {
    			LockManager.LOCK.wait();
        	}
        	readLockCount += 1;
        	lockManager.tIdToLocksMap.get(tId).add(lockManager.pageToLockMap.get(pId));
    		lockManager.pageToPermMap.put(pId, perm);
    		// System.out.printf("I am %s I got the read lock!! read lock:%d write lock:%d\n", Thread.currentThread().getName(), readLockCount, writeLockCount);
		}
    }
    
    public void readUnlock(LockManager lockManager) {
    	synchronized (LockManager.LOCK) {
    		if (readLockCount > 0) {
        		readLockCount -= 1;
        	}
    		// System.out.printf("I am %s, release read lock, read lock:%d write lock:%d\n", Thread.currentThread().getName(), readLockCount, writeLockCount);
    		LockManager.LOCK.notifyAll();
		}
    }
    
    public void writeUnlock(LockManager lockManager) {
    	synchronized (LockManager.LOCK) {
    		if (writeLockCount > 0) {
        		writeLockCount -= 1;
        	}
    		// System.out.printf("I am %s, release write lock, read lock:%d write lock:%d\n", Thread.currentThread().getName(), readLockCount, writeLockCount);
    		LockManager.LOCK.notifyAll();
		}
    }
    
    public int getReadLockNum() {
    	return readLockCount;
    }
    
    public int getWriteLockNum() {
    	return writeLockCount;
    }
    
    public void lockUpgrade(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
    	// 锁升级 释放读锁, 获得写锁并且更新页的读写类型
    	readUnlock(lockManager);
    	writeLock(lockManager, tId, pId, perm);
    }
    
    public void lockDemotion() {
		// 锁降级 some code goes here
	}
    
    public boolean equals(Object obj) {
    	return obj == this;
    }
}
