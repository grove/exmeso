package org.geirove.exmeso.kryo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.geirove.exmeso.ExternalMergeSort;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer<T> implements ExternalMergeSort.Serializer<T> {

    private final Class<T> type;
    private final Kryo kryo;

    public KryoSerializer(Class<T> type) {
        this(type, new Kryo());    
    }

    public KryoSerializer(Class<T> type, Kryo kryo) {
        this.type = type;
        this.kryo = kryo;    
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
