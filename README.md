External Merge Sort
======

This is a small library that implements [External Merge Sort](http://en.wikipedia.org/wiki/External_sorting) in Java.

>"External sorting is a term for a class of sorting algorithms that can handle massive amounts of data. External sorting is required when the data being sorted do not fit into the main memory of a computing device (usually RAM) and instead they must reside in the slower external memory (usually a hard drive). External sorting typically uses a hybrid sort-merge strategy. In the sorting phase, chunks of data small enough to fit in main memory are read, sorted, and written out to a temporary file. In the merge phase, the sorted subfiles are combined into a single larger file. External sorting is a form of distribution sort, with the added characteristic that the individual subsets are separately sorted, rather than just being used internally as intermediate stages." – Wikipedia

### Introduction

The library is pretty flexible as it takes an Iterable&lt;T&gt; as input and returns the sorted result as MergeIterator&lt;T&gt;. 

Sort order is given by an instance of Comparator&lt;T&gt;.

Persistence is handled by an implementation of the Serializer&lt;T&gt; interface, of which there are currently two implementations:

* [JacksonSerializer&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-jackson/src/main/java/org/geirove/exmeso/jackson/JacksonSerializer.java) - serialization and deserialization using the [Jackson](http://jackson.codehaus.org/) library.
* [KryoSerializer&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-kryo/src/main/java/org/geirove/exmeso/kryo/KryoSerializer.java) - serialization and deserialization using the [Kryo](https://code.google.com/p/kryo/) library.

The sorting algorithm is implemented by [ExternalMergeSort&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-core/src/main/java/org/geirove/exmeso/ExternalMergeSort.java).

### Example

Using the Jackson serializer, this code example first reads unsorted input from <code>input.json</code> and then writes the sorted output to <code>output.json</code>. The input is a JSON array of objects that gets sorted by their <code>"id"</code> field in ascending order.

Example input:

    [ {"id" : "c"}, {"id" : "b"}, {"id" : "a"} ]

Example output:

    [ {"id" : "a"}, {"id" : "b"}, {"id" : "c"} ]

<em>Note that it really doesn't make much sense to use this library to sort data this small, or  anything even close. It is meant to be used to sort large amounts of data that cannot all be contained in memory at the same time.</em>

Example code:

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
        sortedChunks = sort.writeSortedChunks(serializer.readValues(input));
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

The above example is a little involved as it needs to, in addition to doing the actual sorting, read the input file and write to the output file. For this it uses the provided serializer directly, which is not always want you want. The input and output data may both be serialized differently, and the format of the internal sorted chunks may be in a third format. The example splits the merge sort in two phases; first write sorted chunks, then create merge iterator over the sorted chunks.

The [ExternalMergeSort&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-core/src/main/java/org/geirove/exmeso/ExternalMergeSort.java) class has the following public instance methods:

    List<File> writeSortedChunks(Iterator<T>)
    MergeIterator<T> mergeSortedChunks(List<File>)
    
    MergeIterator<T> mergeSort(Iterator<T>)

The example used the first two methods, but the third one can also be used instead.

### Helpers

Sometimes, when you don't know the size of your input data, it may not always be neccessary to do an external sort as everything can fit into available memory and be sorted there. This is the case when the size is less than or equal to the <code>chunkSize</code>. For this scenario the [ChunkSizeIterator&lt;T&gt;]() helper class comes in handy. Wrap your own <code>Iterator&lt;T&gt;</code> so that it can figure out if an external merge sort is neccessary:

    Iterator<String> yourIterator = …;
    ChunkSizeIterator csi = new ChunkSizeIterator(yourIterator, chunkSize);
    if (csi.isMultipleChunks()) {
        // do external sort
    } else {
        // do in-memory sort
    }
    
Note that the <code>ExternalMergeSort<T>.mergeSort(Iterator<T>)</code> method will do this for you, so no need to do this if you use that method.
    
### Maven dependencies

#### exmeso-jackson

The <code>exmeso-jackson</code> module can be added to your own project like this:

    <dependency>
      <groupId>org.geirove.exmeso</groupId>
      <artifactId>exmeso-jackson</artifactId>
      <version>0.0.5</version>
    </dependency>

#### exmeso-kryo

The <code>exmeso-kryo</code> module can be added to your own project like this:

    <dependency>
      <groupId>org.geirove.exmeso</groupId>
      <artifactId>exmeso-kryo</artifactId>
      <version>0.0.5</version>
    </dependency>
