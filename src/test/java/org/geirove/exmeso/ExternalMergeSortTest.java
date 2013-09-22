package org.geirove.exmeso;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.geirove.exmeso.ExternalMergeSort.MergeIterator;
import org.junit.Test;

public class ExternalMergeSortTest {

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

    protected ExternalMergeSort<StringPojo> createMergeSort(boolean distinct) {

        JacksonSort<StringPojo> handler = new JacksonSort<StringPojo>(StringPojo.class, new Comparator<StringPojo>() {
            @Override
            public int compare(StringPojo o1, StringPojo o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        return ExternalMergeSort.newSorter(handler)
                .withChunkSize(3)
                .withMaxOpenFiles(2)
                .withDistinct(distinct)
                .withCleanup(true)
                .build();
    }

    @Test
    public void test() throws IOException {
        assertSorted(
                Arrays.asList(B, D, E, C, A), 
                Arrays.asList(A, B, C, D, E), false);
    }

    @Test
    public void testWithDuplicates() throws IOException {
        assertSorted(
                Arrays.asList(B, D, C, B, B, E, A, C, A), 
                Arrays.asList(A, A, B, B, B, C, C, D, E), false);
    }

    @Test
    public void testDistinct() throws IOException {
        assertSorted(
                Arrays.asList(B, D, E, C, A), 
                Arrays.asList(A, B, C, D, E), true);
    }

    @Test
    public void testDistinctWithDuplicates() throws IOException {
        assertSorted(
                Arrays.asList(B, D, C, B, B, E, A, C, A), 
                Arrays.asList(A, B, C, D, E), true);
    }

    private void assertSorted(List<StringPojo> input, List<StringPojo> expected, boolean distinct) throws IOException {
        ExternalMergeSort<StringPojo> sort = createMergeSort(distinct);

        MergeIterator<StringPojo> result = sort.mergeSort(input.iterator());

        Iterator<StringPojo> iter = expected.iterator();
        try {
            while (result.hasNext()) {
                StringPojo r = result.next();
                System.out.println("O: " + r);
                if (!iter.hasNext()) {
                    fail("More than expected");
                }
                StringPojo e = iter.next();
                assertEquals(r, e);
            }
            if (iter.hasNext()) {
                fail("Fewer than expected");
            }
            System.out.println("-----");
        } finally {
            result.close();
        }
    }

}
