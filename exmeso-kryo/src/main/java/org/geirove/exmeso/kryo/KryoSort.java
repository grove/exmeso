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
        this.type = type;
        this.comparator = comparator;
        this.kryo = new Kryo();    
    }
    
    @Override
    public void sortValues(List<T> values) {
        long st = System.currentTimeMillis();
        Collections.sort(values, comparator);
        System.out.println("S: " + (System.currentTimeMillis() - st) + "ms");
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
//            System.out.println("W: " + next);
            kryo.writeObject(output, next);
        }
        output.flush();
        System.out.println("W: " + (System.currentTimeMillis() - st) + "ms");
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
            T next = kryo.readObject(input, type);
//            System.out.println("N: " + next + " " + input.eof() + " " + input.position());
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }
    
}
