package org.geirove.exmeso.msgpack;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.geirove.exmeso.ExternalMergeSort;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

public class MessagePackSerializer<T> implements ExternalMergeSort.Serializer<T> {

    private final Class<T> type;
    private MessagePack msgpack;

    public MessagePackSerializer(Class<T> type) {
        this(type, new MessagePack());    
    }

    public MessagePackSerializer(Class<T> type, MessagePack msgpack) {
        this.type = type;
        this.msgpack = msgpack;    
    }

    @Override
    public void writeValues(Iterator<T> values, OutputStream out) throws IOException {
        Packer packer = msgpack.createPacker(out);
        while (values.hasNext()) {
            packer.write(values.next());
        }
        packer.flush();
    }

    @Override
    public Iterator<T> readValues(InputStream input) throws IOException {
        Unpacker unpacker = msgpack.createUnpacker(input);
        return new MessagePackIterator<T>(unpacker, type, input);
    }

    private static class MessagePackIterator<T> implements Iterator<T> {

        private final Class<T> type;
        private final Unpacker unpacker;
        private T next;

        private MessagePackIterator(Unpacker unpacker, Class<T> type, InputStream input) {
            this.type = type;
            this.unpacker = unpacker;
        }

        @Override
        public boolean hasNext() {
            try {
                unpacker.iterator();
                this.next = unpacker.read(type);
            } catch (EOFException ex) {
                this.next = null;
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return next != null;
        }

        @Override
        public T next() {
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
    }

}
