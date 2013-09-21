package org.geirove.exmeso;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
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
    }

    @Test
    public void test() throws IOException {
        Comparator<StringPojo> comparator = new Comparator<StringPojo>() {
            @Override
            public int compare(StringPojo o1, StringPojo o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        };
        JacksonSort<StringPojo> writer = new JacksonSort<StringPojo>(comparator, StringPojo.class);
        ExternalMergeSort<StringPojo> sort = new ExternalMergeSort<StringPojo>(writer, new File("/tmp"), 2);

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
