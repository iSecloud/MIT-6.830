package simpledb.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import simpledb.common.Permissions;

public class DeadlockManager {
	
	public CopyOnWriteArrayList<TransactionWait> transactionWaitList;
	public ConcurrentHashMap<RWLock, CopyOnWriteArrayList<TransactionWait>> lockWaitListMap;
	
	class TransactionWait {
		public TransactionId tId;
		public Permissions perm;
		public RWLock rwLock;
		
		public TransactionWait(TransactionId tId, RWLock rwLock, Permissions perm) {
			this.tId = tId;
			this.perm = perm;
			this.rwLock = rwLock;
		}
		
		public boolean equals(Object obj) {
			if (!(obj instanceof TransactionWait)) {
				return false;
			}
			TransactionWait trans = (TransactionWait) obj;
			if (trans.tId.equals(tId) && trans.perm.equals(perm) && trans.rwLock.equals(rwLock)) {
				return true;
			}
			return false;
		}
	}
	
	public DeadlockManager() {
		transactionWaitList = new CopyOnWriteArrayList<DeadlockManager.TransactionWait>();
		lockWaitListMap = new ConcurrentHashMap<RWLock, CopyOnWriteArrayList<TransactionWait>>();
	}
	
	public synchronized void deadlockDetected(TransactionId tid, RWLock rwLock, Permissions perm) 
			throws TransactionAbortedException {
		TransactionWait trans = new TransactionWait(tid, rwLock, perm);
		boolean flag = true;
		TransactionWait toDeleteTransactionWait = null;
		for (TransactionWait transIn: transactionWaitList) {
			if (trans.tId.equals(transIn.tId) && trans.rwLock.equals(transIn.rwLock)) {
				if (trans.perm.equals(transIn.perm) || transIn.perm.equals(Permissions.READ_WRITE)) {
					flag = false;
					break;
				} 
				else if (trans.perm.equals(Permissions.READ_WRITE) && !trans.perm.equals(transIn.perm)) {
					// 删除这个trans_wait
					toDeleteTransactionWait = transIn;
					break;
				}
			}
		}
		// TODO 不能直接删除，有些问题
//		if (toDeleteTransactionWait != null) {
//			lockWaitListMap.get(rwLock).remove(toDeleteTransactionWait);
//			transactionWaitList.remove(toDeleteTransactionWait);
//		}
		if (flag) {
			transactionWaitList.add(trans);
			if (!lockWaitListMap.containsKey(rwLock)) {
				lockWaitListMap.put(rwLock, new CopyOnWriteArrayList<DeadlockManager.TransactionWait>());
			}
			lockWaitListMap.get(rwLock).add(trans);
		}
		if (isWaitGraphCycle()) {
			// throw new TransactionAbortedException("Find the existence of a deadlock");
			throw new TransactionAbortedException();
		}
	}
	
//	public void releaseTransLock(TransactionId tId, ConcurrentHashMap<TransactionId, Set<RWLock>> tIdToLocksMap) {
//		for (RWLock rwLock: tIdToLocksMap.get(tId)) {
//			for (TransactionWait trans: transactionWaitList) {
//				if (trans.tId.equals(tId)) {
//					lockWaitListMap.get(rwLock).remove(trans);
//				}	
//			}
//		}
//		for (TransactionWait trans: transactionWaitList) {
//			if (trans.tId.equals(tId)) {
//				transactionWaitList.remove(trans);
//			}
//		}
//	}
	
	public void releaseTransLock(TransactionId tId, RWLock rwLock) {
		for (TransactionWait trans: transactionWaitList) {
			if (trans.tId.equals(tId) && trans.rwLock.equals(rwLock)) {
				lockWaitListMap.get(rwLock).remove(trans);
				transactionWaitList.remove(trans);
			}
		}
	}
	
	public boolean isWaitGraphCycle() {
		// 初始化图数据结构
		HashMap<TransactionId, Integer> transInDegreeMap = new HashMap<TransactionId, Integer>();
		HashMap<TransactionId, ArrayList<TransactionId>> waitGraph = new HashMap<TransactionId, ArrayList<TransactionId>>();
		// 初始化循环等待图
		for (TransactionWait trans: transactionWaitList) {
			transInDegreeMap.put(trans.tId, 0);
		}
		for (TransactionWait transA: transactionWaitList) {
			for (TransactionWait transB: lockWaitListMap.get(transA.rwLock)) {
				if (transA.equals(transB)) {
					break;
				}
				else if (transA.tId.equals(transB.tId)) {
					continue;
				}
				else if (transA.perm.equals(Permissions.READ_WRITE) || !transA.perm.equals(transB.perm)) {
					int inDegree = transInDegreeMap.get(transB.tId) + 1;
					transInDegreeMap.put(transB.tId, inDegree);
					if (!waitGraph.containsKey(transA.tId)) {
						waitGraph.put(transA.tId, new ArrayList<TransactionId>());
					}
					waitGraph.get(transA.tId).add(transB.tId);
				}
			}
		}
		// 利用拓扑排序判断是否有环
		Queue<TransactionId> queue = new LinkedList<TransactionId>();
		for (TransactionId tId: transInDegreeMap.keySet()) {
			int inDegree = transInDegreeMap.get(tId);
			if (inDegree == 0) {
				queue.add(tId);
			}
		}
		while (queue.size() != 0) {
			TransactionId tId = queue.remove();
			if (!waitGraph.containsKey(tId)) {
				continue;
			}
			for (TransactionId adTid: waitGraph.get(tId)) {
				int inDegree = transInDegreeMap.get(adTid) - 1;
				transInDegreeMap.put(adTid, inDegree);
				if (inDegree == 0) {
					queue.add(adTid);
				}
			}
		}
		for (TransactionId tId: transInDegreeMap.keySet()) {
			int inDegree = transInDegreeMap.get(tId);
			if (inDegree != 0) {
				return true;
			}
		}
		return false;
 	}
}
