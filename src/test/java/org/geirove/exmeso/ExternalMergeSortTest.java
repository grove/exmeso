package org.geirove.exmeso;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.geirove.exmeso.ExternalMergeSort.JacksonSort;
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

    @Test
    public void test() throws IOException {
        Comparator<StringPojo> comparator = new Comparator<StringPojo>() {
            @Override
            public int compare(StringPojo o1, StringPojo o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        };
        JacksonSort<StringPojo> handler = new JacksonSort<StringPojo>(comparator, StringPojo.class);
        String tmpdir = System.getProperty("java.io.tmpdir");
        
        ExternalMergeSort<StringPojo> sort = new ExternalMergeSort<StringPojo>(handler, new File(tmpdir), 2);

        List<StringPojo> expected = Arrays.<StringPojo>asList(A, B, C, D, E);
        List<StringPojo> input = Arrays.<StringPojo>asList(B, D, E, C, A);

        MergeIterator<StringPojo> result = sort.mergeSort(input.iterator());
        assertResult(result, expected);
    }

    private void assertResult(MergeIterator<StringPojo> result, List<StringPojo> expected) throws IOException {
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
        } finally {
            result.close();
        }
    }

}
