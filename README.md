External Merge Sort
======

This is a small library that implements [External Merge Sort](http://en.wikipedia.org/wiki/External_sorting). It is pretty flexible as it takes either an InputStream of data or an Iterable&lt;T&gt;. The actual persistence is done by an implementation of the SortHandler interface. There is currently one implementation, JacksonSort, which handles serialization and deserialization using the [Jackson](http://jackson.codehaus.org/) library.

