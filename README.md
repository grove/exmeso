External Merge Sort
======

This is a small library that implements [External Merge Sort](http://en.wikipedia.org/wiki/External_sorting) in Java. 

It is pretty flexible as it takes either an InputStream or an Iterable&lt;T&gt; as input and returns the sorted result as MergeIterator&lt;T&gt;. 

Persistence is handled by an implementation of the SortHandler&lt;T&gt; interface, of which there are currently two implementations:

* [JacksonSort&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-jackson/src/main/java/org/geirove/exmeso/jackson/JacksonSort.java) - serialization and deserialization using the [Jackson](http://jackson.codehaus.org/) library.
* [KryoSort&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-kryo/src/main/java/org/geirove/exmeso/kryo/KryoSort.java) - serialization and deserialization using the [Kryo](https://code.google.com/p/kryo/) library.

The sorting algorithm is implemented by [ExternalMergeSort&lt;T&gt;](https://github.com/grove/exmeso/blob/master/exmeso-core/src/main/java/org/geirove/exmeso/ExternalMergeSort.java).

See the [ExternalMergeSortTest](https://github.com/grove/exmeso/blob/master/exmeso-jackson/src/test/java/org/geirove/exmeso/jackson/ExternalMergeSortTest.java) class for code examples.

Add the following dependency to your own project:

    <dependency>
      <groupId>org.geirove</groupId>
        <artifactId>exmeso-jackson</artifactId>
        <version>0.0.1</version>
      </dependency>

The <code>exmeso-kryo</code> module is not yet released because Kryo version 0.21 contains a bug in its <code>Input.eof()</code> method, so it'll have to wait for the final 0.22 release.

