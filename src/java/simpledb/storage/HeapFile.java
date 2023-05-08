package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final RandomAccessFile raf;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.file = f;
        this.tupleDesc = td;
        try {
            this.raf = new RandomAccessFile(this.file, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        byte[] toRead = new byte[BufferPool.getPageSize()];
        try {
            this.raf.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            this.raf.read(toRead);
            return new HeapPage((HeapPageId) pid, toRead);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        this.raf.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
        this.raf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) Math.ceil(this.file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        HeapPage page = null;
        for (int i = 0; i <= numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            if (i == numPages()) {
                page = new HeapPage(pid, HeapPage.createEmptyPageData());
                writePage(page);
            }
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumUnusedSlots() != 0)
                break;
            Database.getBufferPool().getLockManager().unlock(tid, pid, Permissions.READ_ONLY);
        }
        assert page != null;
        Database.getBufferPool().getLockManager().lock(tid, page.getId(), Permissions.READ_WRITE);
        page.insertTuple(t);
        //LockManager.unlock(tid, page.getId(), Permissions.READ_WRITE);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // TODO: some code goes here
        if (t.getRecordId().getPageId().getTableId() != getId())
            throw new DbException("Not a member of the file");
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
//        LockManager.unlock(tid, pid, Permissions.READ_WRITE);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(this, tid);
    }
}

