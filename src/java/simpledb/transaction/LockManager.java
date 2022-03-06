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
	 * LOCK: ȫ���������ڱ�֤��ȡ/�ͷŶ�д��������һ����
	 * tIdToLocksMap�� ����t�����е�������
	 * pageToLockMap�� ÿ��page��Ӧ����(ÿ��page�������Լ���һ�޶�����)
	 * tIdWatiToGetWriteLock���ȴ�д�������񼯺�
	 * 	 ������ݽṹ��Ϊ�˱�֤ǰ������wait��д���ܼ�ʱ��ã������������������ȡ����������������������
	 *   �ٸ�����: �����ǻ�ȡ����˳��
	 *   r1 r2 w1 w2 r3 w3
	 *   ����r1 r2��ö���û���⣬����1��ȡw����w1ʱû�г��������������ʱ��������������r1�ͷŵ���w1������
	 *   Ȼ���Ի�ȡw2ʱ�ᷢ�ֳ������������������2�ع�����r2���ͷŵ�ͬʱ���ѵȴ���д��w1��
	 *   ������һ�����������ĵط�����������ɲ��ɵ�������r3�õ��˶�������ôw2��������
	 *   Ȼ���Ի�ȡw3�����ʱ�����ܻ�ȡ�ɹ��ģ���Ϊr1���ͷ��ˣ�r2�ع��ˣ�r3�����������������Դ�ʱ�����ڶ�����
	 *   ��ȡ�Ժ�����3���и���д�룬��ʱ��page�Ѿ���Ϊ����ҳ������3�����Ժ�����w1��
	 *   �������ˣ�r1��õ�������w3����ǰ�����ݣ��ʳ���������������
	 *   �������:
	 *   ���һ������t��ȡw�������ˣ��������������������ȡ��/д����ֱ������������w�����������/�ع�
	 * deadlockManager������������
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
			
			// �����ҳ��δ�����������ʼ��һ��ҳ����
			if (!pageToLockMap.containsKey(pageId)) {
				RWLock rwLock = new RWLock();
				pageToLockMap.put(pageId, rwLock);
			}
			// ���������δ�����������ϣ����ʼ��һ��������
			if (!tIdToLocksMap.containsKey(tId)) {
				CopyOnWriteArraySet<RWLock> lockSet = new CopyOnWriteArraySet<RWLock>();
	    		tIdToLocksMap.put(tId, lockSet);
	    	}
			// ����Ƿ�����������
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
