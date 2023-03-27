package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File backingFile;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.backingFile = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.backingFile;
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
        return this.backingFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int fileSize = (int) backingFile.length();
        int pageNo = pid.getPageNumber();
        // System.out.println("Read pageNo: " + pageNo + ", pageId: " + pid.hashCode());
        try {
            RandomAccessFile data = new RandomAccessFile(this.backingFile, "r");
            byte[] pageBytes = new byte[pageSize];
            // Read file per pageSize byte
            for (int i = 0, num = fileSize / pageSize; i < num; i++) {
                data.readFully(pageBytes);
                // Page number matches
                if (i == pageNo) {
                    return new HeapPage((HeapPageId) pid, pageBytes);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new IndexOutOfBoundsException("Page number exceed file size limit");
    }

    private FileInputStream getFis(File f) throws FileNotFoundException {
        return new FileInputStream(this.backingFile);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
            byte[] pageBytes = page.getPageData();
            RandomAccessFile writeFile = new RandomAccessFile(this.backingFile, "rw");
            long positionToWrite = (long) BufferPool.getPageSize() * page.getId().getPageNumber();
            writeFile.seek(positionToWrite);
            writeFile.write(pageBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) backingFile.length() / BufferPool.getPageSize();
    }

    // Insert a tuple into a page in this DbFile
    // If we find an idle page in memory, Mark it dirty; Haven't done anything on disk page
    // else, create a new page, write the space on disk(empty page), mark it dirty
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // Iterate through all pages in this DbFile via BufferPool
        // Find a page with empty slots
        HeapPage chosenPage = null;
        for (int i = 0; i < this.numPages(); i++) {
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (curPage.getNumEmptySlots() > 0) chosenPage = curPage;
        }
        // No idle pages, create a new one
        if (chosenPage == null) {
            HeapPageId hpId = new HeapPageId(this.getId(), numPages());
            Files.write(this.backingFile.toPath(), HeapPage.createEmptyPageData(), StandardOpenOption.APPEND);
            chosenPage = (HeapPage) Database.getBufferPool().getPage(tid, hpId, Permissions.READ_WRITE);
        }
        // Insert tuple and mark the page as dirty
        chosenPage.insertTuple(t);
        chosenPage.markDirty(true, tid);

        return new ArrayList<>(List.of(chosenPage));
    }

    // Delete a tuple on the specific page in memory(BufferPool cache)
    // Mark it dirty; Hasn't done anything on disk page
    // Will need to flush/write the modification on disk
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pageId = t.recordId.getPageId();
        // Read via bufferPool, make sure the page will cache in memory
        HeapPage affectedPage = (HeapPage) Database.getBufferPool().getPage(
                tid, pageId, Permissions.READ_WRITE);

        // Delete tuple and mark the page as dirty
        affectedPage.deleteTuple(t);
        affectedPage.markDirty(true, tid);

        return new ArrayList<>(List.of(affectedPage));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            int pageNo;
            BufferPool bp;
            HeapPageId curPageId;
            Iterator<Tuple> curPageIter;
            HeapPage curPage;
            boolean isOpen = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                // Use global BufferPool
                bp = Database.getBufferPool();
                isOpen = true;
                setIteratorPageNo(0);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) throw new DbException("Iterator not open yet");
                // Check if there's a next tuple in this page
                if (curPageIter.hasNext()) return true;
                // Check if there's a valid next page
                boolean hasNextPage = false;
                for (int i = this.pageNo + 1; i < numPages(); i++) {
                    HeapPage curPage = (HeapPage) bp.getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
                    if (curPage.getNumEmptySlots() < curPage.numSlots) {
                        hasNextPage = true;
                        break;
                    }
                }
                return hasNextPage;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen) throw new NoSuchElementException("Iterator not open yet");
                if (hasNext()) {
                    // End of this page, move to next one
                    // Jump over all empty pages
                    while (hasNext() && !curPageIter.hasNext()) {
                        setIteratorPageNo(this.pageNo + 1);
                    }
                    return curPageIter.next();
                } else throw new NoSuchElementException("Pages out of limits");
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // Reset fields
                setIteratorPageNo(0);
            }

            @Override
            public void close() {
                isOpen = false;
            }

            /**
             *
             * @param _pageNo
             *      Target pageNo that we need to retrieve
             * Set up HeapPageId, HeapPage and the iterator for this HeapPage
             * set the _pageNo field in those instance properly
             */
            private void setIteratorPageNo(int _pageNo) throws TransactionAbortedException, DbException {
                this.pageNo = _pageNo;
                this.curPageId = new HeapPageId(getId(), pageNo);
                this.curPage = (HeapPage) bp.getPage(tid, curPageId, Permissions.READ_ONLY);
                this.curPageIter = curPage.iterator();
            }
        };
    }

}

