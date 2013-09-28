package org.geirove.exmeso.jackson;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.geirove.exmeso.ExternalMergeSort;

public class JacksonSerializer<T> implements ExternalMergeSort.Serializer<T> {

    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper() {{
        configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }};

    private final Class<T> type;
    private final ObjectMapper mapper;

    public JacksonSerializer(Class<T> type) {
        this(type, DEFAULT_MAPPER);
    }

    public JacksonSerializer(Class<T> type, ObjectMapper mapper) {
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public void writeValues(Iterator<T> values, OutputStream out) throws IOException{
        long st = System.currentTimeMillis();
        JsonFactory jsonFactory = mapper.getJsonFactory();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(out);
        jsonGenerator.writeStartArray();
        while (values.hasNext()) {
            T next = values.next();
            jsonGenerator.writeObject(next);
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
        if (ExternalMergeSort.debug) {
            System.out.println("W: " + (System.currentTimeMillis() - st) + "ms");
        }
    }

    @Override
    public Iterator<T> readValues(InputStream input) throws IOException {
        JsonFactory jsonFactory = mapper.getJsonFactory();
        JsonParser jsonParser = jsonFactory.createJsonParser(input);
        jsonParser.nextToken(); // skip JsonToken.START_ARRAY
        return jsonParser.readValuesAs(type);
    }

    @Override
    public void close() throws IOException {
    }

}