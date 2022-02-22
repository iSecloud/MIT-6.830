package simpledb.transaction;

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
    
    public synchronized void lock(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
    	// �����ǰ�ж���������д���������������
    	if (isLockUpgrade(lockManager, tId, pId, perm)) {
    		lockUpgrade(lockManager, tId, pId, perm);
    	}
    	// ����ظ����ͬһ����/�ͼ�����������
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
    
    public synchronized void writeLock(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
//    	long begin = System.currentTimeMillis();
//    	long rest = RESET;
    	while (writeLockCount != 0 || readLockCount != 0) {
//    		this.wait(rest);
//    		rest = rest - (System.currentTimeMillis() - begin);
    		this.wait();
    	}
//    	if (rest < 0) {
//    		throw new TransactionAbortedException();
//    	}
    	writeLockCount += 1;
    	lockManager.tIdToLocksMap.get(tId).add(lockManager.pageToLockMap.get(pId));
		lockManager.pageToPermMap.put(pId, perm);
    }
    
    public synchronized void readLock(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
//    	long begin = System.currentTimeMillis();
//    	long rest = RESET;
    	while (writeLockCount != 0) {
//    		this.wait(rest);
//    		rest = rest - (System.currentTimeMillis() - begin);
    		this.wait();
    	}
//    	if (rest < 0) {
//    		throw new TransactionAbortedException();
//    	}
    	readLockCount += 1;
    	lockManager.tIdToLocksMap.get(tId).add(lockManager.pageToLockMap.get(pId));
		lockManager.pageToPermMap.put(pId, perm);
    }
    
    public synchronized void readUnlock() {
    	if (readLockCount > 0) {
    		readLockCount -= 1;
    	}
    	this.notifyAll();
    }
    
    public synchronized void writeUnlock() {
    	if (writeLockCount > 0) {
    		writeLockCount -= 1;
    	}
    	this.notifyAll();
    }
    
    public int getReadLockNum() {
    	return readLockCount;
    }
    
    public synchronized void lockUpgrade(LockManager lockManager, TransactionId tId, PageId pId, Permissions perm) 
    		throws InterruptedException, TransactionAbortedException {
    	// ������ �ͷŶ���, ���д�����Ҹ���ҳ�Ķ�д����
    	readUnlock();
    	writeLock(lockManager, tId, pId, perm);
    }
    
    public void lockDemotion() {
		// ������ some code goes here
	}
    
    public boolean equals(Object obj) {
    	return obj == this;
    }
}
