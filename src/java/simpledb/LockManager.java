package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;

public class LockManager {
    HashMap<TransactionId, PageId> waitlist;
    HashMap<PageId, Permissions> pageLockMap;
    HashMap<TransactionId, Set<PageId>> tidToPages;
    HashMap<PageId, Set<TransactionId>> pageToTids;


    Permissions SHARED_LOCK = Permissions.READ_ONLY;
    Permissions EXCLUSIVE_LOCK = Permissions.READ_WRITE;

    public LockManager() {
        this.pageLockMap = new HashMap<>();
        this.waitlist = new HashMap<>();
        this.tidToPages = new HashMap<>();
        this.pageToTids = new HashMap<>();
    }

    /**
     * tid tries to require a perm-level-lock on page pid
     * return true if the lock is successfully acquired; otherwise return false
     */
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws IOException, TransactionAbortedException {
        // Add the request to waitlist
        // Detect deadlock
        if (!waitlist.containsKey(tid)) {
            detectDeadlock(tid, pid, perm);
            waitlist.put(tid, pid);
        }

        // Check if acquire lock is available
        if (!isLockAvailable(tid, pid, perm)) return false;
        // Update lock manager
        if (!tidToPages.containsKey(tid)) tidToPages.put(tid, new HashSet<>());
        if (!pageToTids.containsKey(pid)) pageToTids.put(pid, new HashSet<>());

        // Prevent SHARED overwrite EXCLUSIVE lock
        if (!pageLockMap.containsKey(pid) || !pageLockMap.get(pid).equals(EXCLUSIVE_LOCK))
            pageLockMap.put(pid, perm);

        // Set automatically ignore replicates
        tidToPages.get(tid).add(pid);
        pageToTids.get(pid).add(tid);
        waitlist.remove(tid);
        return true;
    }

    /**
     * Given tid, pid and perm, return if target lock is available to grant
     */
    public synchronized boolean isLockAvailable(TransactionId tid, PageId pid, Permissions perm) {

        if (!pageLockMap.containsKey(pid)) return true;

        // SHARED X EXCLUSIVE lock logic
        Permissions curLockType = pageLockMap.get(pid);
        int numOfLockHolders = 0;
        if (pageToTids.containsKey(pid)) numOfLockHolders = pageToTids.get(pid).size();

        // SHARED - SHARED : Grant the lock
        // SHARED - EXCLUSIVE : Grant & Upgrade only when this tid is the only holder for this SHARED lock
        if (curLockType.equals(SHARED_LOCK)) {
            if (perm.equals(SHARED_LOCK)) return true;
            else return pageToTids.get(pid).contains(tid) && numOfLockHolders < 2;
        }
        // EXCLUSIVE - SHARED: Grant only when this tid holds this EXCLUSIVE lock
        // EXCLUSIVE - EXCLUSIVE: Grant only when this tid holds this EXCLUSIVE lock
        else return pageToTids.get(pid).contains(tid);
    }

    /**
     * Given tid, pid, return if the specific tid hold a lock on Page pid
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        if (!pageLockMap.containsKey(pid) || !tidToPages.containsKey(tid))
            return false;

        return tidToPages.get(tid).contains(pid) && tidToPages.get(tid).contains(pid);
    }

    /**
     * Release the lock from pid corresponding to tid
     * There could be multi-thread releasing Transactions and releasing locks at the same time
     * Which means we might pass in a pid that has already been released
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!pageLockMap.containsKey(pid) || !tidToPages.containsKey(tid) ||
                !tidToPages.get(tid).contains(pid)) {
            return;
        }

        pageToTids.get(pid).remove(tid);
        if (pageToTids.get(pid).isEmpty()) {
            pageToTids.remove(pid);
            pageLockMap.remove(pid);
        }

        tidToPages.get(tid).remove(pid);
        if (tidToPages.get(tid).isEmpty()) {
            tidToPages.remove(tid);
        }
    }

    /**
     * Release the transaction with all its locks
     */
    public synchronized void releaseTransaction(TransactionId tid) {
        // Must initialize a new Set, To avoid : ConcurrentModificationException(tidToPages.values())
        Set<PageId> releasePidSet = new HashSet<>();
        if (tidToPages.containsKey(tid)) releasePidSet.addAll(tidToPages.get(tid));
        for (PageId pid : releasePidSet) releaseLock(tid, pid);
        waitlist.remove(tid);
    }

    // TODO: BFS -> pageToTids -> waitingList -> ...
    public synchronized void detectDeadlock(TransactionId tid, PageId pid, Permissions type) throws
            TransactionAbortedException, IOException {
        // Page(pid) already been locked by tid with type Permission,
        // No need for deadlock detect
        if (tidToPages.containsKey(tid) && tidToPages.get(tid).contains(pid) &&
                type.equals(pageLockMap.get(pid))) return;

        // Detect circle in graph using BFS
        Queue<PageId> bfs = new LinkedList<>();
        bfs.offer(pid);
        int bfsDepth = 0;

        while (!bfs.isEmpty()) {
            PageId curPid = bfs.poll();
            if (!pageToTids.containsKey(curPid)) continue;
            Set<TransactionId> nextTidSet = new HashSet<>(pageToTids.get(curPid));
            for (TransactionId nextTid : nextTidSet) {
                // Find a circle
                if (nextTid.equals(tid)) {
                    // Prevent self-loop-circle in a multi-shared scenario
                    if (bfsDepth == 0) continue;
                    // Deadlock
                    System.out.println("Deadlock detected ... tid: " + tid + " pid: " + pid + " with perm: " + type);
                    throw new TransactionAbortedException("Deadlock...");
                }
                if (waitlist.containsKey(nextTid)) bfs.offer(waitlist.get(nextTid));
            }
            bfsDepth++;
        }
    }
}
