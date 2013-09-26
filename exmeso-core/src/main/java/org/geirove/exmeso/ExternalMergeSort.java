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

    public static boolean debug = false;
    public static boolean debugMerge = false;
    
    private final SortHandler<T> handler;
    private final Config<T> config;

    private ExternalMergeSort(Config<T> config) {
        this.config = config;
        this.handler = config.handler;
    }
    
    public static <T> Config<T> newSorter(SortHandler<T> handler) {
        return new Config<T>(handler);
    }

    public static class Config<T> {
        
        private final SortHandler<T> handler;
        private File tempDirectory;
        private int maxOpenFiles = 25;
        private int chunkSize = 1000;
        private int bufferSize = 8192;
        private boolean cleanup = true;
        private boolean distinct = false;
        
        private Config(SortHandler<T> handler) {
            this.handler = handler;
        }
        
        public Config<T> withTempDirectory(File tempDirectory) {
            this.tempDirectory = tempDirectory;
            return this;
        }
        
        public Config<T> withMaxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return this;
        }
        
        public Config<T> withChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }
        
        public Config<T> withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }
        
        public Config<T> withDistinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }
        
        public Config<T> withCleanup(boolean cleanup) {
            this.cleanup = cleanup;
            return this;
        }
        
        public ExternalMergeSort<T> build() {
            if (tempDirectory == null) {
                String tmpdir = System.getProperty("java.io.tmpdir");
                this.tempDirectory = new File(tmpdir);
            }
            return new ExternalMergeSort<T>(this);
        }
    }
    
    public static interface SortHandler<T> extends Closeable {

        void sortValues(List<T> values);

        int compareValues(T o1, T o2);

        void writeValues(Iterator<T> values, OutputStream out) throws IOException;

        Iterator<T> readValues(InputStream input) throws IOException;
        
    }

    public MergeIterator<T> mergeSort(Iterator<T> values) throws IOException {
        List<File> files = partialMerge(writeSortedChunks(values));
        return merge(files);
    }

    public MergeIterator<T> mergeSort(InputStream input) throws IOException {
        List<File> files = partialMerge(writeSortedChunks(input));
        return merge(files);
    }

    private MergeIterator<T> merge(List<File> sortedChunks) throws IOException {
        return new MergeIterator<T>(sortedChunks, handler, config.cleanup, config.distinct, config.bufferSize);
    }

    private List<File> partialMerge(List<File> sortedChunks) throws IOException {
        int chunks = sortedChunks.size();

        if (chunks > config.maxOpenFiles) {
            if (debugMerge) {
                System.out.println("----------------------------");
                System.out.println("chunks: " + chunks);
            }
            // partial merge down to maxOpenFiles
            int merges = (chunks-(chunks % config.maxOpenFiles)) / config.maxOpenFiles;
            int chunksNotMerged = chunks % merges;
            int chunksMerged = chunks - chunksNotMerged;
            int subsequentMerges = chunksMerged / merges;
            int firstMerge = subsequentMerges + (chunksMerged % merges);
     
            if (debugMerge) {
                System.out.printf("merges: %d = (%d - (%d %% %d)) / %d\n", merges, chunks, chunks, config.maxOpenFiles, config.maxOpenFiles);
                if (merges > 1) {
                    System.out.printf("chunksNotMerged: %d = %d %% %d\n", chunksNotMerged, chunks, merges);
                    System.out.printf("chunksMerged: %d = %d - %d\n", chunksMerged, chunks, chunksNotMerged);
                    System.out.printf("firstMerge: %d = %d + (%d %% %d)\n", firstMerge, subsequentMerges, chunksMerged, merges);
                    System.out.printf("subsequentMerges: %d = %d / %d\n", subsequentMerges, chunksMerged, merges);
                }
            }
            
            List<File> result = new ArrayList<File>();

            // first merge
            List<File> subList = sortedChunks.subList(0, firstMerge);
            if (debugMerge) {
                System.out.println("first slice: " + 0 + "-" + firstMerge);
            }
            result.add(mergeSubList(result, subList));

            // subsequent merges
            for (int i=0; i < merges-1; i++) {
                int offset = firstMerge + (i * subsequentMerges);
                if (debugMerge) {
                    System.out.println("subsequent slice: " + offset + "-" + (offset + subsequentMerges));
                }
                subList = sortedChunks.subList(offset, offset + subsequentMerges);
                result.add(mergeSubList(result, subList));
            }
            
            int lastOffset = firstMerge + ((merges-1)*subsequentMerges);
            result.addAll(sortedChunks.subList(lastOffset, chunks));
            if (debugMerge) {
                System.out.println("afterPartialMergeChunks: " + result.size());
            }
            return partialMerge(result);
        }
        return sortedChunks;
    }

    private File mergeSubList(List<File> result, List<File> subList)
            throws IOException {
        MergeIterator<T> iter = merge(subList);
        try {
            return writeChunk("exmeso-merged-", iter);
        } finally {
            iter.close();
        }
    }

    public static class MergeIterator<T> implements Iterator<T>, Closeable {

        private final PriorityQueue<ChunkFile<T>> pq;
        private final List<ChunkFile<T>> cfs;

        private final boolean cleanup;
        private final boolean distinct;

        private T next;

        MergeIterator(List<File> files, SortHandler<T> handler, boolean cleanup, boolean distinct, int bufferSize) throws IOException {
            this.cleanup = cleanup;
            this.distinct = distinct;
            List<ChunkFile<T>> cfs = new ArrayList<ChunkFile<T>>(files.size());
            for  (File file : files) {
                cfs.add(new ChunkFile<T>(file, handler, bufferSize));
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
            IOException ex = null;
            for (ChunkFile<T> cf : cfs) {
                try {
                    cf.close();
                    if (cleanup) {
                        cf.delete();
                    }
                } catch (IOException e) {
                    ex = e; 
                }
            }
            if (ex != null) {
                throw ex;
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
            return handler.compareValues(next, o.next);
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        public void delete() {
            file.delete();
        }

    }

    protected File createChunkFile(String prefix) throws IOException {
        File result = File.createTempFile(prefix, "", config.tempDirectory);
        if (debug) {
            System.out.println("F: " + result);
        }
        return result;
    }

    private List<File> writeSortedChunks(InputStream input) throws IOException {
        List<File> result = new ArrayList<File>();
        Iterator<T> iter = handler.readValues(input);
        List<T> chunk = new ArrayList<T>(Math.max(2, config.chunkSize/4));
        while (iter.hasNext()) {
            chunk.add(iter.next());
            if (chunk.size() > config.chunkSize) {
                File chunkFile = writeSortedChunk(chunk);
                result.add(chunkFile);
                chunk = new ArrayList<T>(Math.max(2, config.chunkSize/4));
            }
        }
        if (!chunk.isEmpty()) {
            File chunkFile = writeSortedChunk(chunk);
            result.add(chunkFile);
        }
        return result;
    }

    private List<File> writeSortedChunks(Iterator<T> input) throws IOException {
        List<File> result = new ArrayList<File>();
        while (input.hasNext()) {
            List<T> chunk = readChunk(input);
            File chunkFile = writeSortedChunk(chunk);
            result.add(chunkFile);
        }
        return result;
    }

    private File writeSortedChunk(List<T> values) throws IOException {
        handler.sortValues(values);
        return writeChunk("exmeso-sorted-", values.iterator());
    }

    private File writeChunk(String prefix, Iterator<T> values) throws IOException {
        File chunkFile = createChunkFile(prefix);
        OutputStream out = new BufferedOutputStream(new FileOutputStream(chunkFile), config.bufferSize);
        try {
            handler.writeValues(values, out);
        } finally {
            out.close();
        }
        return chunkFile;
    }

    private List<T> readChunk(Iterator<T> input) {
        List<T> result = new ArrayList<T>(Math.max(2, config.chunkSize/4));
        int c = 0;
        while (input.hasNext()) {
            c++;
            result.add(input.next());
            if (c >= config.chunkSize) {
                return result;
            }
        }
        return result;
    }

}
