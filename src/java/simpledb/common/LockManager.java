package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class LockManager {

    private Map<PageId, Lock> pidSLock = new HashMap<>();
    private Map<TransactionId, Map<PageId, Count>> tidSLock = new HashMap<>();
    private Map<TransactionId, Set<PageId>> dirtyPages = new HashMap<>();

//    public static void resetLockManager() {
//        pidSLock = new HashMap<>();
//        tidSLock = new HashMap<>();
//        dirtyPages = new HashMap<>();
//    }

    public LockManager() {
        this.pidSLock = new HashMap<>();
        this.tidSLock = new HashMap<>();
        this.dirtyPages = new HashMap<>();
    };

    private class Lock {
        private AtomicInteger read;
        private AtomicInteger write;
        private PageId pid;
        private final List<TransactionId> readLockQueue = new ArrayList<>();
        private final List<TransactionId> writeLockQueue = new ArrayList<>();
        private final List<TransactionId> readLockWaitingQueue = new ArrayList<>();
        private final List<TransactionId> writeLockWaitingQueue = new ArrayList<>();

        public Lock(PageId pid) {
            this.read = new AtomicInteger();
            this.write = new AtomicInteger();
            this.pid = pid;
        }

        public synchronized int getReadLock() {
            return this.read.intValue();
        }

        public synchronized int getWriteLock() {
            return this.write.intValue();
        }

        public synchronized void rLock(TransactionId tid) throws TransactionAbortedException {
            while (getWriteLock() != 0 && tidSLock.get(tid).get(pid).getWrite() != getWriteLock()) {
                try {
                    readLockWaitingQueue.add(tid);
                    detectCycle(tid, Permissions.READ_ONLY);
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (TransactionAbortedException e) {
                    readLockWaitingQueue.remove(tid);
                    throw e;
                } finally {
                    this.notifyAll();
                }
            }
            this.read.incrementAndGet();
            tidSLock.get(tid).get(pid).read.incrementAndGet();
            readLockQueue.add(tid);
            this.notifyAll();
        }

        public synchronized void rUnlock(TransactionId tid) {
            this.read.decrementAndGet();
            tidSLock.get(tid).get(pid).read.decrementAndGet();
            readLockQueue.remove(tid);
            this.notifyAll();
        }

        public void rUnlockAll(TransactionId tid, int times) {
            while (times > 0) {
                rUnlock(tid);
                times--;
            }
            while(readLockWaitingQueue.contains(tid)) {
                readLockWaitingQueue.remove(tid);
            }
        }

        public synchronized void wLock(TransactionId tid) throws TransactionAbortedException {
            while((getWriteLock() != tidSLock.get(tid).get(pid).getWrite()) || (getReadLock() != tidSLock.get(tid).get(pid).getRead())) {
                try {
                    writeLockWaitingQueue.add(tid);
                    detectCycle(tid, Permissions.READ_WRITE);
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (TransactionAbortedException e) {
                    writeLockWaitingQueue.remove(tid);
                    throw e;
                } finally {
                    this.notifyAll();
                }
            }
            // TODO: decide if or not to give the lock or abort
            this.write.incrementAndGet();
            tidSLock.get(tid).get(pid).write.incrementAndGet();
            dirtyPages.get(tid).add(pid);
            writeLockQueue.add(tid);
            this.notifyAll();
        }

        public synchronized void wUnlock(TransactionId tid) {
            this.write.decrementAndGet();
            tidSLock.get(tid).get(pid).write.decrementAndGet();
            writeLockQueue.remove(tid);
            this.notifyAll();
        }

        public void wUnlockAll(TransactionId tid, int times) {
            while (times > 0) {
                wUnlock(tid);
                times --;
            }
            while (writeLockWaitingQueue.contains(tid)) {
                writeLockWaitingQueue.remove(tid);
            }
        }

        public void detectCycle(TransactionId tid, Permissions perm) throws TransactionAbortedException, InterruptedException {
            Set<TransactionId> currentLevel = new LinkedHashSet<>();
//            switch (perm) {
//                case READ_ONLY:
//                    currentLevel.addAll(readDependency());
//                    break;
//                case READ_WRITE:
//                    currentLevel.addAll(writeDependency());
//                    break;
//            }
            currentLevel.add(tid);
            while (currentLevel.size() > 0) {
                Set<TransactionId> nextLevel = new LinkedHashSet<>();
                Iterator<TransactionId> it = currentLevel.iterator();
                while (it.hasNext()) {
                    TransactionId curTid = it.next();
                    for (Map.Entry<PageId, Count> entry : tidSLock.get(curTid).entrySet()) {
                        PageId k = entry.getKey();
                        Count v = entry.getValue();
                        Lock lock = pidSLock.get(k);
                        if (lock.isWaitingRead(curTid)) {
                            if (lock.readDependency().contains(tid)) {
                                throw new TransactionAbortedException();
                            }
                            nextLevel.addAll(lock.readDependency());
                        }
                        if (lock.isWaitingWrite(curTid)) {
                            if (lock.writeDependency().contains(tid)) {
//                                if (curTid == tid) {
//                                    return;
//                                }
                                throw new TransactionAbortedException();
                            }
                            nextLevel.addAll(lock.writeDependency());
                        }
                    }
                }
                currentLevel = nextLevel;
            }
        }

        /**
         * Return all the transactions that may block a read request,
         * which is all the transactions that currently hold a write lock.
         * @return a set of transactions which hold write lock
         */
        public Set<TransactionId> readDependency() {
            Set<TransactionId> rd = new HashSet<>();
            if (writeLockQueue.size() > 0) {
                rd.addAll(writeLockQueue);
            }
            return rd;
        }

        /**
         * Return all the transactions that may block a write request,
         * which is all the transactions that currently hold a read lock or
         * a write lock.
         * @return a set of transactions which hold read / write lock.
         */
        public Set<TransactionId> writeDependency() {
            Set<TransactionId> rd = new LinkedHashSet<>();
            rd.addAll(writeLockQueue);
            rd.addAll(readLockQueue);
            return rd;
        }

        public synchronized boolean isWaitingRead(TransactionId tid) {
            return readLockWaitingQueue.contains(tid);
        }

        public synchronized boolean isWaitingWrite(TransactionId tid) {
            return writeLockWaitingQueue.contains(tid);
        }

        @Override
        public String toString() {
            return "PageId: " + pid +
                    ", ReadCount: " + getReadLock() +
                    ", WriteCount: " + getWriteLock() +
                    ", readQueue: " + readLockQueue +
                    ", writeQueue: " + writeLockQueue +
                    ", readWait: " + readLockWaitingQueue +
                    ", writeWait: " + writeLockWaitingQueue;
        }
    }

    private static class Count {
        public AtomicInteger read;
        public AtomicInteger write;
        public TransactionId tid;

        public Count(TransactionId tid) {
            this.read = new AtomicInteger();
            this.write = new AtomicInteger();
            this.tid = tid;
        }

        public int getRead() {
            return this.read.intValue();
        }

        public int getWrite() {
            return this.write.intValue();
        }

        @Override
        public String toString() {
            return "TransactionId: " + tid +
                    ", ReadCount: " + getRead() +
                    ", WriteCount: " + getWrite();
        }
    }

    public void lock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (!pidSLock.containsKey(pid)) {
            pidSLock.put(pid, new Lock(pid));
        }
        if (!tidSLock.containsKey(tid)) {
            tidSLock.put(tid, new HashMap<>());
        }
        if (!tidSLock.get(tid).containsKey(pid)) {
            tidSLock.get(tid).put(pid, new Count(tid));
        }
        if (!dirtyPages.containsKey(tid)) {
            dirtyPages.put(tid, new HashSet<>());
        }

        switch (perm) {
            case READ_ONLY:
                pidSLock.get(pid).rLock(tid);
                break;
            case READ_WRITE:
                pidSLock.get(pid).wLock(tid);
                break;
        }
    }

    public void unlock(TransactionId tid, PageId pid, Permissions perm) {
        switch (perm) {
            case READ_ONLY:
                pidSLock.get(pid).rUnlock(tid);
                break;
            case READ_WRITE:
                pidSLock.get(pid).wUnlock(tid);
                break;
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        pidSLock.get(pid).rUnlockAll(tid, tidSLock.get(tid).get(pid).getRead());
        pidSLock.get(pid).wUnlockAll(tid, tidSLock.get(tid).get(pid).getWrite());
    }

    public void releaseAllLocks(TransactionId tid) {
        tidSLock.getOrDefault(tid, new HashMap<>()).forEach((k, v) -> {
            releaseLock(tid, k);
        });
    }

    public Set<PageId> getDirtyPages(TransactionId tid) {
        return dirtyPages.getOrDefault(tid, new HashSet<>());
    }
    public void resetDirtyPages(TransactionId tid) {
        dirtyPages.put(tid, new HashSet<>());
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return tidSLock.containsKey(tid) &&
                tidSLock.get(tid).containsKey(pid) &&
                (tidSLock.get(tid).get(pid).getRead() != 0 ||
                tidSLock.get(tid).get(pid).getWrite() != 0);
    }

    public boolean hasWriteLock(PageId pid) {
        return pidSLock.get(pid).getWriteLock() != 0;
    }

    public synchronized void printLocks() {
        System.out.println("tidlock" + tidSLock);
        System.out.println("pidlock" + pidSLock);
    }

    public synchronized void printActiveLocks(TransactionId tid) {
        tidSLock.get(tid).forEach((k, v) -> {
            if (holdsLock(tid, k)) {
                System.out.println("Pid: " + k + "Count: " + v);
            }
        });
    }
}
