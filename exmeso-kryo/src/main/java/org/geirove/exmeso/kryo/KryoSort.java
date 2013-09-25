package org.geirove.exmeso.kryo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.geirove.exmeso.ExternalMergeSort;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSort<T> implements ExternalMergeSort.SortHandler<T> {

    private final Class<T> type;
    private final Comparator<T> comparator;
    private final Kryo kryo;

    public KryoSort(Class<T> type, Comparator<T> comparator) {
        this(type, comparator, new Kryo());    
    }

    public KryoSort(Class<T> type, Comparator<T> comparator, Kryo kryo) {
        this.type = type;
        this.comparator = comparator;
        this.kryo = new Kryo();    
    }
    
    @Override
    public void sortValues(List<T> values) {
        long st = System.currentTimeMillis();
        Collections.sort(values, comparator);
        if (ExternalMergeSort.debug) {
            System.out.println("S: " + (System.currentTimeMillis() - st) + "ms");
        }
    }

    @Override
    public int compareValues(T o1, T o2) {
        return comparator.compare(o1, o2);
    }

    @Override
    public void writeValues(Iterator<T> values, OutputStream out) throws IOException {
        long st = System.currentTimeMillis();
        Output output = new Output(out);
        while (values.hasNext()) {
            T next = values.next();
            kryo.writeObject(output, next);
        }
        output.flush();
        if (ExternalMergeSort.debug) {
            System.out.println("W: " + (System.currentTimeMillis() - st) + "ms");
        }
    }

    @Override
    public Iterator<T> readValues(InputStream input) throws IOException {
        return new KryoIterator<T>(kryo, type, input);
    }

    @Override
    public void close() throws IOException {
    }

    private static class KryoIterator<T> implements Iterator<T> {

        private final Kryo kryo;
        private final Class<T> type;
        private final Input input;

        private KryoIterator(Kryo kryo, Class<T> type, InputStream in) {
            this.kryo = kryo;
            this.type = type;
            this.input = new Input(in);
        }

        @Override
        public boolean hasNext() {
            return !input.eof();
        }

        @Override
        public T next() {
            return kryo.readObject(input, type);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
}
