package org.geirove.exmeso;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class ExternalMergeSort<T> {

    private final SortHandler<T> handler;
    private final File tmpdir;

    private int maxOpenFiles = 25;
    private int chunkSize = 1000;

    private int bufferSize = 8192;

    private boolean deleteOnClose = true;
    private boolean distinct = false;

    public ExternalMergeSort(SortHandler<T> handler, File tmpdir, int chunkSize, boolean distinct) {
        this.handler = handler;
        this.tmpdir = tmpdir;
        this.chunkSize = chunkSize;
        this.distinct = distinct;
    }

    public static interface SortHandler<T> extends Closeable {

        void sortChunk(List<T> values);

        int compareChunks(T o1, T o2);

        void writeChunk(Iterator<T> values, OutputStream out) throws IOException;
        void writeChunkValue(T value, OutputStream out) throws IOException;

        Iterator<T> readValues(InputStream input) throws IOException;
    }

    public MergeIterator<T> mergeSort(Iterator<T> values) throws IOException {
        List<File> files = partialMerge(sortChunks(values));
        return mergeSort(files);
    }

    public MergeIterator<T> mergeSort(InputStream input) throws IOException {
        List<File> files = partialMerge(sortChunks(input));
        return mergeSort(files);
    }

    private MergeIterator<T> mergeSort(List<File> sortedChunks) throws IOException {
        return new MergeIterator<T>(sortedChunks, handler, deleteOnClose, distinct, bufferSize);
    }

    private List<File> partialMerge(List<File> sortedChunks) throws IOException {
        int size = sortedChunks.size();
        if (size > maxOpenFiles) {
            List<File> result = new ArrayList<File>();
            for (int i=0; i < size; i += maxOpenFiles) {
                List<File> subList = sortedChunks.subList(i, Math.min(i+ maxOpenFiles, size));
                MergeIterator<T> iter = mergeSort(subList);
                try {
                    File newChunk = writeChunk(iter);
                    result.add(newChunk);
                } finally {
                    iter.close();
                }
            }
            return partialMerge(result);
        }
        return sortedChunks;
    }

    public static class MergeIterator<T> implements Iterator<T>, Closeable {

        private final PriorityQueue<ChunkFile<T>> pq;
        private final List<ChunkFile<T>> cfs;

        private final boolean deleteOnClose;
        private boolean distinct;

        private T next;

        MergeIterator(List<File> files, SortHandler<T> handler, boolean deleteOnClose, boolean distinct, int bufferSize) throws IOException {
            this.deleteOnClose = deleteOnClose;
            this.distinct = distinct;
            List<ChunkFile<T>> cfs = new ArrayList<ChunkFile<T>>(files.size());
            for  (File file : files) {
                ChunkFile<T> cf = new ChunkFile<T>(file, handler, bufferSize);
                cfs.add(cf);
            }
            this.cfs = cfs;
            this.pq = new PriorityQueue<ChunkFile<T>>(cfs.size(), new Comparator<ChunkFile<T>>() {
                @Override
                public int compare(ChunkFile<T> o1, ChunkFile<T> o2) {
                    return o1.compareTo(o2);
                }
            });
            pq.addAll(cfs);
            readNext();
        }

        private void readNext() {
            T next_;
            if (pq.isEmpty()) {
                next_ = null;
            } else {
                if (distinct) {
                    do {
                        ChunkFile<T> cf = pq.poll();
                        next_ = cf.pop();
                        if (!cf.isEmpty()) {
                            pq.add(cf);
                        }
                        if (!next_.equals(next)) {
                            break;
                        }
                    } while (true);
                } else {
                    ChunkFile<T> cf = pq.poll();
                    next_ = cf.pop();
                    if (!cf.isEmpty()) {
                        pq.add(cf);
                    }
                }
            }
            this.next = next_;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public T next() {
            T result = next;
            readNext();
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            for (ChunkFile<T> cf : cfs) {
                cf.close();
                if (deleteOnClose) {
                    cf.delete();
                }
            }
        }

    }

    private static class ChunkFile<T> implements Comparable<ChunkFile<T>>, Closeable {

        private final File file;
        private final InputStream input;
        private final SortHandler<T> handler;

        private Iterator<T> iter;
        private T next;

        private ChunkFile(final File file, SortHandler<T> handler, int bufferSize) throws IOException {
            this.file = file;
            this.handler = handler;
            input = new BufferedInputStream(new FileInputStream(file), bufferSize);
            iter = handler.readValues(input);
            readNext();
        }

        @Override
        public String toString() {
            return "Chunk[next=" + next + ", file=" + file + "]";
        }

        private void readNext() {
            this.next = iter.hasNext() ? iter.next() : null;
        }

        public boolean isEmpty() {
            return next == null;
        }

        public T pop() {
            T result = next;
            readNext();
            return result;
        }

        @Override
        public int compareTo(ChunkFile<T> o) {
            return handler.compareChunks(next, o.next);
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        public void delete() {
            file.delete();
        }

    }

    private List<File> sortChunks(InputStream input) throws IOException {
        List<File> result = new ArrayList<File>();
        Iterator<T> iter = handler.readValues(input);
        List<T> chunk = new ArrayList<T>(Math.max(2, chunkSize/4));
        while (iter.hasNext()) {
            chunk.add(iter.next());
            if (chunk.size() > chunkSize) {
                File chunkFile = sortAndWriteChunk(chunk);
                result.add(chunkFile);
                chunk = new ArrayList<T>(Math.max(2, chunkSize/4));
            }
        }
        if (!chunk.isEmpty()) {
            File chunkFile = sortAndWriteChunk(chunk);
            result.add(chunkFile);
        }
        return result;
    }

    private List<File> sortChunks(Iterator<T> input) throws IOException {
        List<File> result = new ArrayList<File>();
        while (input.hasNext()) {
            List<T> chunk = readChunk(input);
            File chunkFile = sortAndWriteChunk(chunk);
            result.add(chunkFile);
        }
        return result;
    }

    protected File createChunkFile() throws IOException {
        return File.createTempFile("exmeso-", "", tmpdir);
    }

    private File sortAndWriteChunk(List<T> values) throws IOException {
        handler.sortChunk(values);
        return writeChunk(values.iterator());
    }

    private File writeChunk(Iterator<T> values) throws IOException {
        File chunkFile = createChunkFile();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(chunkFile), bufferSize);
        try {
            handler.writeChunk(values, out);
        } finally {
            out.close();
        }
        return chunkFile;
    }

    private List<T> readChunk(Iterator<T> input) {
        List<T> result = new ArrayList<T>(Math.max(2, chunkSize/4));
        int c = 0;
        while (input.hasNext()) {
            c++;
            result.add(input.next());
            if (c >= chunkSize) {
                return result;
            }
        }
        return result;
    }

}
