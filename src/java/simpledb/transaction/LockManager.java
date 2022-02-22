package simpledb.transaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hamcrest.core.IsInstanceOf;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {
	
	public ConcurrentHashMap<TransactionId, Set<RWLock>> tIdToLocksMap;
	public ConcurrentHashMap<PageId, RWLock> pageToLockMap;
	public ConcurrentHashMap<PageId, Permissions> pageToPermMap;
	public DeadlockManager deadlockManager;
	
	public LockManager() {
		tIdToLocksMap = new ConcurrentHashMap<TransactionId, Set<RWLock>>();
		pageToLockMap = new ConcurrentHashMap<PageId, RWLock>();
		pageToPermMap = new ConcurrentHashMap<PageId, Permissions>();
		deadlockManager = new DeadlockManager();
	}
	
	public void lock(TransactionId tId, PageId pageId, Permissions perm) throws TransactionAbortedException {
		// 如果该页面未曾加锁，则初始化一个页级锁
		if (!pageToLockMap.containsKey(pageId)) {
			RWLock rwLock = new RWLock();
			pageToLockMap.put(pageId, rwLock);
		}
		// 如果该事务未曾持有锁集合，则初始化一个锁集合
		if (!tIdToLocksMap.containsKey(tId)) {
    		Set<RWLock> lockSet = new HashSet<RWLock>();
    		tIdToLocksMap.put(tId, lockSet);
    	}
		// 检测是否有死锁存在
		deadlockManager.deadlockDetected(tId, pageToLockMap.get(pageId), perm);
		try {
			pageToLockMap.get(pageId).lock(this, tId, pageId, perm);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void unlock(TransactionId tId, PageId pId) {
		if (!pageToLockMap.containsKey(pId)) {
        	return;
        } 
        RWLock rwLock = pageToLockMap.get(pId);
        if (!tIdToLocksMap.get(tId).contains(rwLock)) {
        	return;
        }
		if (pageToPermMap.get(pId).equals(Permissions.READ_ONLY)) {
			rwLock.readUnlock();
			if (rwLock.getReadLockNum() == 0) {
				pageToPermMap.remove(pId);
			}
    	} else {
    		rwLock.writeUnlock();
    		pageToPermMap.remove(pId);
    	}
		deadlockManager.releaseTransLock(tId, rwLock);
		tIdToLocksMap.get(tId).remove(pageToLockMap.get(pId));
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
	
//	public void releaseTransWaitLock(TransactionId tId) {
//		deadlockManager.releaseTransLock(tId, tIdToLocksMap);
//	}
}
