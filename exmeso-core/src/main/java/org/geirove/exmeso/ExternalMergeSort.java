package org.geirove.exmeso;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * An implementation of External Merge Sort. This class has a fluent API for building an 
 * instance. The created instance has several methods for doing external merge sort in 
 * one or two steps depending on which method is most appropriate for the situation. 
 * 
 * @author grove@geirove.org
 *
 * @param <T> The type of values to sort.
 */
public class ExternalMergeSort<T> {

    public static boolean debug = false;
    public static boolean debugMerge = false;
    
    private final Config<T> config;
    private final Serializer<T> serializer;
    private final Comparator<T> comparator;

    private ExternalMergeSort(Config<T> config) {
        this.config = config;
        this.serializer = config.serializer;
        this.comparator = config.comparator;
    }
    
    /**
     * Fluent API building a new instance of ExternalMergeSort<T.
     * @param serializer Serializer<T> to use when sorting.
     * @return Config instance that can be used to set options and in the end create a new instance.
     */
    public static <T> Config<T> newSorter(Serializer<T> serializer, Comparator<T> comparator) {
        return new Config<T>(serializer, comparator);
    }

    public static class Config<T> {
        
        private final Serializer<T> serializer;
        private final Comparator<T> comparator;

        private File tempDirectory;
        private int maxOpenFiles = 25;
        private int chunkSize = 1000;
        private boolean cleanup = true;
        private boolean distinct = true;
        
        private Config(Serializer<T> serializer, Comparator<T> comparator) {
            this.serializer = serializer;
            this.comparator = comparator;
        }
        
        /**
         * Specifies which directory to use when storing temporary files. The 
         * default is System.getProperty("java.io.tmpdir").
         * @param tempDirectory The temporary directory.
         * @return this
         */
        public Config<T> withTempDirectory(File tempDirectory) {
            this.tempDirectory = tempDirectory;
            return this;
        }

        /**
         * Specifies the maximum number of open files that can be used 
         * to read and write files. The default is 25. 
         * @param maxOpenFiles The maximum number of open files.
         * @return this
         */
        public Config<T> withMaxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return this;
        }
        
        /**
         * Specifies the maxiumum number of objects in each chunk files. It 
         * is also the number of objects to sort in memory at one time.
         * @param chunkSize The maximum number of objects in a chunk file. The
         * default is 1000.
         * @return this
         */
        public Config<T> withChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }
        
        /**
         * Specifies whether to remove duplicate values. The default is true.
         * @param distinct If true then remove duplicate values.
         * @return this
         */
        public Config<T> withDistinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }
        
        /**
         * Specifies whether to remove temporary files when 
         * MergeIterator.close() is called. The default is true.
         * @param cleanup If true then remove temporary files
         * @return this
         */
        public Config<T> withCleanup(boolean cleanup) {
            this.cleanup = cleanup;
            return this;
        }
        
        /**
         * Create an instance of ExternalMergeSort with the 
         * given configuration options.
         * @return An instance of ExternalMergeSort<T>.
         */
        public ExternalMergeSort<T> build() {
            if (tempDirectory == null) {
                String tmpdir = System.getProperty("java.io.tmpdir");
                this.tempDirectory = new File(tmpdir);
            }
            return new ExternalMergeSort<T>(this);
        }
    }
    
    /**
     * An interface implemented by serializers that serialize and deserialize 
     * objects to be sorted. It is also responsible for doing the actual 
     * sorting of objects.
     *
     * @param <T> The type of objects to be sorted.
     */
    public static interface Serializer<T> extends Closeable {

        void writeValues(Iterator<T> values, OutputStream out) throws IOException;

        Iterator<T> readValues(InputStream input) throws IOException;
        
    }

    /**
     * Performs an external merge on the values in the iterator.
     * @param values Iterator containing the data to sort.
     * @return an iterator the iterates over the sorted result.
     * @throws IOException if something fails when doing I/O.
     */
    public MergeIterator<T> mergeSort(Iterator<T> values) throws IOException {
        ChunkSizeIterator<T> csi = new ChunkSizeIterator<T>(values, config.chunkSize);
        if (csi.isMultipleChunks()) {
            List<File> sortedChunks = writeSortedChunks(csi);
            return mergeSortedChunks(sortedChunks);
        } else {
            List<T> list = new ArrayList<T>(csi.getHeadSize());
            while (csi.hasNext()) {
                list.add(csi.next());
            }
            Collections.sort(list, comparator);
            return new CollectionMergeIterator<T>(list.iterator());
        }
    }
    
    private static class CollectionMergeIterator<T> implements MergeIterator<T> {

        private final Iterator<T> nested;

        private CollectionMergeIterator(Iterator<T> nested) {
            this.nested = nested;
        }
        
        @Override
        public boolean hasNext() {
            return nested.hasNext();
        }

        @Override
        public T next() {
            return nested.next();
        }

        @Override
        public void remove() {
            nested.remove();
        }

        @Override
        public void close() throws IOException {
            // nothing to do here
        }
        
    }
    
    /**
     * Returns an iterator over the sorted result. Takes a list of already sorted chunk files as 
     * input. Note that this method is normally used with one of the writeSortedChunks methods.
     * @param sortedChunks a list of sorted chunk files
     * @return an iterator the iterates over the sorted result.
     * @throws IOException if something fails when doing I/O.
     */
    public MergeIterator<T> mergeSortedChunks(List<File> sortedChunks) throws IOException {
        return mergeSortedChunksNoPartialMerge(partialMerge(sortedChunks));
    }
    
    private MergeIterator<T> mergeSortedChunksNoPartialMerge(List<File> sortedChunks) throws IOException {
        if (debugMerge) {
            System.out.println("Merging chunks: " + sortedChunks.size());
        }
        if (sortedChunks.size() == 1) {
            File sortedChunk = sortedChunks.get(0);
            return new ChunkFile<T>(sortedChunk, serializer, comparator);
        } else {
            return new MultiFileMergeIterator<T>(sortedChunks, serializer, comparator, config.cleanup, config.distinct);
        }
    }

    private List<File> partialMerge(List<File> sortedChunks) throws IOException {
        List<File> result = sortedChunks;
        while (result.size() > config.maxOpenFiles) {
            if (debugMerge) {
                System.out.println("----------------------------");
                System.out.println("Partial merge start: " + result.size());
            }
            result = partialSubMerge(result);
            if (debugMerge && result.size() <= config.maxOpenFiles) {
                System.out.println("Partial merge end: " + result.size());
            }
        }
        return result;
    }
    
    private List<File> partialSubMerge(List<File> sortedChunks) throws IOException {
        int chunks = sortedChunks.size();

        if (chunks > config.maxOpenFiles) {
            
            int chunksToMerge = Math.min(Math.max(2, chunks - config.maxOpenFiles), config.maxOpenFiles);

            if (debugMerge) {
                System.out.printf("chunks: %d\n", chunks);
                System.out.printf("chunksToMerge: %d = Math.min(Math.max(2 - %d), %d)\n", chunksToMerge, chunks, config.maxOpenFiles, config.maxOpenFiles);
            }

            List<File> headList = sortedChunks.subList(0, chunksToMerge);
            List<File> tailList = sortedChunks.subList(chunksToMerge, sortedChunks.size());
            if (debugMerge) {
                System.out.printf("head: %d tail: %d\n", headList.size(), tailList.size());
            }

            List<File> result = new ArrayList<File>(tailList);
            result.add(mergeSubList(headList));
            
            if (debugMerge) {
                System.out.printf("chunks result: %d\n", result.size());
            }
            
            return result;
        }
        return sortedChunks;
    }

    private File mergeSubList(List<File> subList) throws IOException {
        MergeIterator<T> iter = mergeSortedChunksNoPartialMerge(subList);
        try {
            return writeChunk("exmeso-merged-", iter);
        } finally {
            iter.close();
        }
    }
    
    public static interface MergeIterator<T> extends Iterator<T>, Closeable {
    }
    
    private static class MultiFileMergeIterator<T> implements MergeIterator<T> {

        private final Queue<ChunkFile<T>> pq;
        private final List<ChunkFile<T>> cfs;

        private final boolean cleanup;
        private final boolean distinct;

        private T next;

        private MultiFileMergeIterator(List<File> files, Serializer<T> serializer, Comparator<T> comparator, boolean cleanup, boolean distinct) throws IOException {
            this.cleanup = cleanup;
            this.distinct = distinct;
            List<ChunkFile<T>> cfs = new ArrayList<ChunkFile<T>>(files.size());
            for  (File file : files) {
                cfs.add(new ChunkFile<T>(file, serializer, comparator));
            }
            this.cfs = cfs;
            int initialSize = Math.max(1, cfs.size());
            this.pq = new PriorityQueue<ChunkFile<T>>(initialSize, new Comparator<ChunkFile<T>>() {
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
                        next_ = cf.next();
                        if (cf.hasNext()) {
                            pq.add(cf);
                        }
                        if (!next_.equals(next)) {
                            break;
                        }
                    } while (true);
                } else {
                    ChunkFile<T> cf = pq.poll();
                    next_ = cf.next();
                    if (cf.hasNext()) {
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

    private static class ChunkFile<T> implements Comparable<ChunkFile<T>>, MergeIterator<T> {

        private final File file;
        private final Comparator<T> comparator;

        private final InputStream input;
        private Iterator<T> iter;
        private T next;

        private ChunkFile(final File file, Serializer<T> serializer, Comparator<T> comparator) throws IOException {
            this.file = file;
            this.comparator = comparator;
            input = new FileInputStream(file);
            iter = serializer.readValues(input);
            readNext();
        }

        @Override
        public String toString() {
            return "Chunk[next=" + next + ", file=" + file + "]";
        }

        private void readNext() {
            this.next = iter.hasNext() ? iter.next() : null;
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
        public int compareTo(ChunkFile<T> o) {
            return comparator.compare(next, o.next);
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

    /**
     * Reads the data from the iterator and writes sorted chunk files to disk.
     * @param input Iterator containing the data to sort.
     * @return list of sorted chunk files. 
     * @throws IOException if something fails when doing I/O.
     */
    public List<File> writeSortedChunks(Iterator<T> input) throws IOException {
        List<File> result = new ArrayList<File>();
        while (input.hasNext()) {
            List<T> chunk = readChunk(input);
            File chunkFile = writeSortedChunk(chunk);
            result.add(chunkFile);
        }
        if (debugMerge) {
            System.out.printf("Chunks %d (chunkSize=%d, maxOpenFiles=%d)\n", result.size(), config.chunkSize, config.maxOpenFiles);
        }
        return result;
    }

    private File writeSortedChunk(List<T> values) throws IOException {
        long st = System.currentTimeMillis();
        Collections.sort(values, comparator);
        if (ExternalMergeSort.debug) {
            System.out.println("S: " + (System.currentTimeMillis() - st) + "ms");
        }
        return writeChunk("exmeso-sorted-", values.iterator());
    }

    private File writeChunk(String prefix, Iterator<T> values) throws IOException {
        File chunkFile = createChunkFile(prefix);
        OutputStream out = new FileOutputStream(chunkFile);
        try {
            serializer.writeValues(values, out);
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
