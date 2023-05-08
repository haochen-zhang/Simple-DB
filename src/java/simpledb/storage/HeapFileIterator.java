package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements the interface of DbFileIterator for HeapFile
 */
public class HeapFileIterator implements DbFileIterator {

    private final HeapFile heapFile;
    private final TransactionId tid;
    private int pageNumber;
    private Iterator<Tuple> iter;
    private boolean open;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.heapFile = f;
        this.tid = tid;
        this.pageNumber = 0;
        this.open = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException{
        this.open = true;
        iter = getTupleIterator(this.pageNumber);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iter == null) {
            return false;
        }

        while (!iter.hasNext()) {
            this.pageNumber++;
            if (this.pageNumber >= this.heapFile.numPages()) {
                return false;
            }
            iter = getTupleIterator(this.pageNumber);
        }
        return true;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return iter.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.pageNumber = 0;
        iter = getTupleIterator(this.pageNumber);
    }

    @Override
    public void close() {
        this.open = false;
        iter = null;
    }

    private Iterator<Tuple> getTupleIterator(int pageNumber)
            throws TransactionAbortedException, DbException{
//        HeapPage page = (HeapPage) Database.getBufferPool().getPage(
//                this.transactionId, (PageId) (new HeapPageId(this.heapFile.getId(), pageNumber)), Permissions.READ_ONLY);
//
//        return page.iterator();
        if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
            HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }else{
            throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
        }
    }
}

