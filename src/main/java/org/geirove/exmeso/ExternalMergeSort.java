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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

public class ExternalMergeSort<T> {

    private final SortHandler<T> handler;
    private final File tmpdir;

    @SuppressWarnings("unused")
    private boolean distinct;

    private int bufferSize = 8192;
    private int chunkSize = 1000;
    private boolean deleteOnClose = false;
    
    public ExternalMergeSort(SortHandler<T> handler, File tmpdir, int chunkSize) {
        this.handler = handler;
        this.tmpdir = tmpdir;
        this.chunkSize = chunkSize;
    }

    public static interface SortHandler<T> extends Closeable {

        void sortChunk(List<T> values);

        int compareChunks(T o1, T o2);

        void writeChunk(List<T> values, OutputStream out) throws IOException;
        void writeChunkValue(T value, OutputStream out) throws IOException;

        Iterator<T> readValues(InputStream input) throws IOException;
    }

    public static class JacksonSort<T> implements SortHandler<T> {

        private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper() {{
            configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
            configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        }};

        private final ObjectMapper mapper;
        private final Comparator<T> comparator;
        private final Class<T> type;

        public JacksonSort(Comparator<T> comparator, Class<T> type) {
            this(DEFAULT_MAPPER, comparator, type);
        }

        public JacksonSort(ObjectMapper mapper, Comparator<T> comparator, Class<T> type) {
            this.mapper = mapper;
            this.comparator = comparator;
            this.type = type;
        }

        @Override
        public void writeChunk(List<T> values, OutputStream out) throws IOException{
            for (T value : values) {
                writeChunkValue(value, out);
            }
        }

        @Override
        public void writeChunkValue(T value, OutputStream out) throws IOException{
            mapper.writeValue(out, value);
        }

        @Override
        public int compareChunks(T o1, T o2) {
            return comparator.compare(o1, o2);
        }
        
        @Override
        public void sortChunk(List<T> values) {
            Collections.sort(values, comparator);
        }

        @Override
        public Iterator<T> readValues(InputStream input) throws IOException {
            JsonFactory jfactory = mapper.getJsonFactory();
            JsonParser jParser = jfactory.createJsonParser(input);
            return jParser.readValuesAs(type);
        }

        @Override
        public void close() throws IOException {
        }

    }

//    private void mergeFiles(Iterator<T> values, OutputStream out) throws IOException {
//        List<File> files = sortChunks(iter);
//        MergeIterator<T> iter = mergeIterator(files);
//        try {
//            while (iter.hasNext()) {
//                T value = iter.next();
//                handler.writeChunkValue(value, out);
//            }
//        } finally {
//            iter.close();
//        }
//    }

    public MergeIterator<T> mergeSort(Iterator<T> values) throws IOException {
        List<File> files = sortChunks(values);
        return new MergeIterator<T>(files, handler, deleteOnClose);
    }

    public MergeIterator<T> mergeSort(InputStream input) throws IOException {
        List<File> files = sortChunks(input);
        return new MergeIterator<T>(files, handler, deleteOnClose);
    }

    private static class MergeIterator<T> implements Iterator<T>, Closeable {
        
        private final PriorityQueue<ChunkFile<T>> pq;
        private final List<ChunkFile<T>> cfs;
        private final boolean deleteOnClose;
        
        private T next;
        
        MergeIterator(List<File> files, SortHandler<T> handler, boolean deleteOnClose) throws IOException {
            this.deleteOnClose = deleteOnClose;
            List<ChunkFile<T>> cfs = new ArrayList<ChunkFile<T>>(files.size());
            for  (File file : files) {
                ChunkFile<T> cf = new ChunkFile<T>(file, handler);
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
//            System.out.println("X1: " + next);
            T next_;
            if (pq.isEmpty()) {
                next_ = null;
            } else {
                ChunkFile<T> cf = pq.poll();
                next_ = cf.pop();
                if (!cf.isEmpty()) {
                    pq.add(cf);
                }
            }
            this.next = next_;
//            System.out.println("X2: " + next);
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
        
        private ChunkFile(final File file, SortHandler<T> handler) throws IOException {
            this.file = file;
            this.handler = handler;
            input = new BufferedInputStream(new FileInputStream(file), 8192) {
//                @Override
//                public int read() throws IOException {
//                    int c = super.read();
//                    System.out.println(file + " c1: " + (char)c);
//                    return c;
//                }
//                @Override
//                public int read(byte[] b) throws IOException {
//                    int c = super.read(b);
//                    System.out.println(file + "c2: " + new String(b));
//                    return c;
//                }
//                @Override
//                public int read(byte[] b, int off, int len) throws IOException {
//                    int c = super.read(b, off, len);
//                    System.out.println(file + "c3: " + new String(b) + " " + off + " " + len + " " + c);
//                    return c;
//                }
//                @Override
//                public void close() throws IOException {
//                    super.close();
//                }
            };
            iter = handler.readValues(input);
            readNext();
        }

        @Override
        public String toString() {
            return "Chunk[next=" + next + ", file=" + file + "]";
        }
        
        private void readNext() {
            this.next = iter.hasNext() ? iter.next() : null;
//            System.out.println("G: " + next);
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
            int c = handler.compareChunks(next, o.next);
//            System.out.println("" + c + " " + next + " " + o.next);
            return c;
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
                File chunkFile = writeChunk(chunk);
                result.add(chunkFile);
                chunk = new ArrayList<T>(Math.max(2, chunkSize/4));
            }
        }
        if (!chunk.isEmpty()) {
            File chunkFile = writeChunk(chunk);
            result.add(chunkFile);
        }
        return result;
    }

    private List<File> sortChunks(Iterator<T> input) throws IOException {
        List<File> result = new ArrayList<File>();
        while (input.hasNext()) {
            List<T> chunk = readChunk(input);
            File chunkFile = writeChunk(chunk);
            result.add(chunkFile);
        }
        return result;
    }

    protected File createChunkFile() throws IOException {
        return File.createTempFile("exmeso-", "", tmpdir);
    }
    
    private File writeChunk(List<T> values) throws IOException {
        File chunkFile = createChunkFile();
        OutputStream out = new BufferedOutputStream(new FileOutputStream(chunkFile), bufferSize);
        try {
            try {
                handler.sortChunk(values);
                handler.writeChunk(values, out);
            } finally {
                handler.close();
            }
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

    // -- tests

    public static class StringPojo {
        private String value;
        public StringPojo(@JsonProperty("value") String value) {
            this.value = value;
        }
        public String getValue() {
            return value;
        }
        public String toString() {
            return "{\"value\":" + value + "\"}";
        }
    }

    public static void main(String[] args) throws Exception {
        // receive iterator of objects and write to file(s) of approxiately specified size

        Comparator<StringPojo> comparator = new Comparator<StringPojo>() {
            @Override
            public int compare(StringPojo o1, StringPojo o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        };
        JacksonSort<StringPojo> writer = new JacksonSort<StringPojo>(comparator, StringPojo.class);
        ExternalMergeSort<StringPojo> sort = new ExternalMergeSort<StringPojo>(writer, new File("/tmp"), 2);

        // load chunk array from iterator
        List<StringPojo> chunk = Arrays.<StringPojo>asList(new StringPojo("B"), new StringPojo("D"), new StringPojo("E"), new StringPojo("C"), new StringPojo("A"));

        MergeIterator<StringPojo> iter = sort.mergeSort(chunk.iterator());
        try {
            while (iter.hasNext()) {
                StringPojo o = iter.next();
                System.out.println("O: " + o);
            }
        } finally {
            iter.close();
        }
    }

}
