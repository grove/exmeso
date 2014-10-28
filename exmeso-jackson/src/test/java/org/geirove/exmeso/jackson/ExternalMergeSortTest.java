package org.geirove.exmeso.jackson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.geirove.exmeso.AbstractExternalMergeSortTest;
import org.geirove.exmeso.ExternalMergeSort;
import org.geirove.exmeso.CloseableIterator;
import org.junit.Test;

public class ExternalMergeSortTest extends AbstractExternalMergeSortTest {

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
        public boolean equals(Object o) {
            if (o instanceof StringPojo) {
                StringPojo other = (StringPojo)o;
                return other.value.equals(value);
            }
            return false;
        }
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static final StringPojo A = new StringPojo("A");
    private static final StringPojo B = new StringPojo("B");
    private static final StringPojo C = new StringPojo("C");
    private static final StringPojo D = new StringPojo("D");
    private static final StringPojo E = new StringPojo("E");

    @Test
    public void testEmpty() throws IOException {
        assertSorted(
                Arrays.<StringPojo>asList(), 
                Arrays.<StringPojo>asList(), false);
    }

    @Test
    public void testSmallBelowChunkSize() throws IOException {
        assertSorted(
                Arrays.asList(B, A), 
                Arrays.asList(A, B), false);
    }

    @Test
    public void testSmallEqualsChunkSize() throws IOException {
        assertSorted(
                Arrays.asList(B, C, A), 
                Arrays.asList(A, B, C), false);
    }

    @Test
    public void testSmallAboveChunkSize() throws IOException {
        assertSorted(
                Arrays.asList(B, C, D, A), 
                Arrays.asList(A, B, C, D), false);
    }

    @Test
    public void testNonDistinct() throws IOException {
        assertSorted(
                Arrays.asList(B, D, E, C, A), 
                Arrays.asList(A, B, C, D, E), false);
    }

    @Test
    public void testNonDistinctWithDuplicates() throws IOException {
        assertSorted(
                Arrays.asList(B, D, C, B, B, E, A, C, A), 
                Arrays.asList(A, A, B, B, B, C, C, D, E), false);
    }

    @Test
    public void testDistinct() throws IOException {
        int chunkSize = 2;
        int maxOpenFiles = 2;
        assertSorted(
                Arrays.asList(B, D, E, C, A), 
                Arrays.asList(A, B, C, D, E), true, chunkSize, maxOpenFiles);
    }

    @Test
    public void testDistinctSingleChunk() throws IOException {
        int chunkSize = 1000;
        int maxOpenFiles = 2;
        assertSorted(
                Arrays.asList(B, D, E, C, A), 
                Arrays.asList(A, B, C, D, E), true, chunkSize, maxOpenFiles);
    }

    @Test
    public void testDistinctWithDuplicates() throws IOException {
        int chunkSize = 2;
        int maxOpenFiles = 2;
        assertSorted(
                Arrays.asList(B, D, C, B, B, E, A, C, A), 
                Arrays.asList(A, B, C, D, E), true, chunkSize, maxOpenFiles);
    }

    @Test
    public void testDistinctWithDuplicatesSingleChunk() throws IOException {
        int chunkSize = 1000;
        int maxOpenFiles = 2;
        assertSorted(
                Arrays.asList(B, D, C, B, B, E, A, C, A), 
                Arrays.asList(A, B, C, D, E), true, chunkSize, maxOpenFiles);
    }

    private void assertSorted(List<StringPojo> input, List<StringPojo> expected, boolean distinct) throws IOException {
        assertSorted(input, expected, distinct, 3, 2);
    }
    
    private void assertSorted(List<StringPojo> input, List<StringPojo> expected, boolean distinct,
            int chunkSize, int maxOpenFiles) throws IOException {
        ExternalMergeSort<StringPojo> sort = createMergeSort(distinct, chunkSize, maxOpenFiles);

        CloseableIterator<StringPojo> result = sort.mergeSort(input.iterator());

        Iterator<StringPojo> iter = expected.iterator();
        try {
            while (result.hasNext()) {
                StringPojo r = result.next();
                if (!iter.hasNext()) {
                    fail("More than expected");
                }
                StringPojo e = iter.next();
                assertEquals(r, e);
            }
            if (iter.hasNext()) {
                fail("Fewer than expected");
            }
        } finally {
            result.close();
        }
    }

    private static final Comparator<StringPojo> stringPojoComparator = new Comparator<StringPojo>() {
        @Override
        public int compare(StringPojo o1, StringPojo o2) {
            return o1.getValue().compareTo(o2.getValue());
        }
    };
    private static final JacksonSerializer<StringPojo> stringPojoSerializer = new JacksonSerializer<StringPojo>(StringPojo.class);
    
    protected ExternalMergeSort<StringPojo> createMergeSort(boolean distinct, int chunkSize, int maxOpenFiles) {

        return ExternalMergeSort.newSorter(stringPojoSerializer, stringPojoComparator)
                .withChunkSize(chunkSize)
                .withMaxOpenFiles(maxOpenFiles)
                .withDistinct(distinct)
                .withCleanup(!ExternalMergeSort.debug)
                .build();
    }
    
    private final static Comparator<Integer> integerComparator = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    };
    
    private final static JacksonSerializer<Integer> integerSerializer = new JacksonSerializer<Integer>(Integer.class);

    @Test
    @Override
    public void testLargeIntegerSort() throws IOException {
        performLargeIntegerSort(integerSerializer, integerComparator, false);
    }
    
    @Test
    public void testPrimeIntegerSort() throws IOException {
        performPrimeIntegerSort(integerSerializer, integerComparator, false);
    }

    @Test
    public void testMultiMergeIntegerSort() throws IOException {
        performMultiMergeIntegerSort(integerSerializer, integerComparator, false);
    }

}
