External Merge Sort
======

This is a small library that implements [External Merge Sort](http://en.wikipedia.org/wiki/External_sorting) in Java. 

It is pretty flexible as it takes either an InputStream or an Iterable&lt;T&gt; as input and returns the sorted result as MergeIterator&lt;T&gt;. 

Persistence is handled by an implementation of the Serializer&lt;T&gt; interface, of which there are currently two implementations:

* [JacksonSerializer&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-jackson/src/main/java/org/geirove/exmeso/jackson/JacksonSerializer.java) - serialization and deserialization using the [Jackson](http://jackson.codehaus.org/) library.
* [KryoSerializer&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-kryo/src/main/java/org/geirove/exmeso/kryo/KryoSerializer.java) - serialization and deserialization using the [Kryo](https://code.google.com/p/kryo/) library.

The sorting algorithm is implemented by [ExternalMergeSort&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-core/src/main/java/org/geirove/exmeso/ExternalMergeSort.java).

### Maven dependency

Add the following dependency to your own project:

    <dependency>
      <groupId>org.geirove.exmeso</groupId>
      <artifactId>exmeso-jackson</artifactId>
      <version>0.0.3</version>
    </dependency>

The <code>exmeso-kryo</code> module is not yet released because Kryo version 0.21 contains a bug in its <code>Input.eof()</code> method, so it'll have to wait for the final 0.22 release. Modules released to Maven Central cannot reference SNAPSHOT dependencies. For now you'll have to build the module locally if you would like to test it.

### Example code

This code example reads unsorted input from <code>input.json</code> and writes the sorted output to <code>output.json</code>. The JSON input is an array of JSON objects that gets sorted by their <code>"id"</code> field.

    // Prepare input and output files
    File inputFile = new File("/tmp/input.json");
    File outputFile = new File("/tmp/output.json");
    
    // Create comparator
    Comparator<ObjectNode> comparator = new Comparator<ObjectNode>() {
        @Override
        public int compare(ObjectNode o1, ObjectNode o2) {
            String id1 = o1.path("id").getTextValue();
            String id2 = o2.path("id").getTextValue();
            return id1.compareTo(id2);
        }
    };
    
    // Create serializer
    JacksonSerializer<ObjectNode> serializer = new JacksonSerializer<ObjectNode>(ObjectNode.class);

    // Create the external merge sort instance
    ExternalMergeSort<ObjectNode> sort = ExternalMergeSort.newSorter(serializer, comparator)
            .withChunkSize(1000)
            .withMaxOpenFiles(10)
            .withDistinct(true)
            .withCleanup(true)
            .withTempDirectory(new File("/tmp"))
            .build();
   
    // Read input file as an input stream and write sorted chunks.
    List<File> sortedChunks;
    InputStream input = new FileInputStream(inputFile);
    try {
        sortedChunks = sort.writeSortedChunks(input);
    } finally {
        input.close();
    }
    
    // Get a merge iterator over the sorted chunks. This will return the
    // objects in sorted order. Note that the sorted chunks will be deleted 
    // when the MergeIterator is closed if 'cleanup' is set to true.
    MergeIterator<ObjectNode> sorted = sort.mergeSortedChunks(sortedChunks);
    try {
        OutputStream output = new FileOutputStream(outputFile);
        try {
            serializer.writeValues(sorted, output);
        } finally {
            output.close();
        }
    } finally {
        sorted.close();
    }


See also the [ExternalMergeSortTest](https://github.com/grove/exmeso/blob/master/exmeso-jackson/src/test/java/org/geirove/exmeso/jackson/ExternalMergeSortTest.java) class for more code examples.

