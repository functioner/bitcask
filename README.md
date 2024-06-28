# bitcask
A pared down version of the bitcask storage engine

## How to build and run
To build the package with tests:
```
mvn clean package
```
Or to build the package without tests:
```
mvn clean package -DskipTests
```

To run the package:
server (blocking in the terminal):
```
./bin/run.sh --server ./datadir
```

Note that `datadir` is just an example. You can use your own path name.
If the path does not exists, then it will create a new KV.
Otherwise, it will load the KV in this path.

client:
```
./bin/run.sh --client
```

Note that the client is just an example (random read or write).
You can easily modify the code and implement your own client.

## Design

The `bitcask` folder contains the implementation of the KV store.
The `app` folder contains applications using the bitcask library.
When `value` is null, then `put(key, value)` means delete the key.

There are 4 main components in this bitcask implementation.
1. network service: based on Java JMI. Code files: `KVStoreStub.java` and `KVStoreRemote.java`
2. in-memory KV: based on ConcurrentHashMap in JDK. The read/write/delete in different keys are concurrent. Code files: `KeyDir.java`
3. write of data file in disk: a sychronized sequantial write. Code files: `PersistentData.java` and `DataFileEntry.java`
4. concurrent read of data file in disk: parallel readers of all data files in disk. Code files: `FileReader.java`

Fail-recover mechanism: every time an entry (<=4K) is written to disk, it will flush, which is atomic operation.
Either recovering from before the operation or after the operation works well. If the entry > 4K (and <8K), then flush before 4K and flush again after finish.
Then if the content after 4K is loss, we just directly abandon the first 4K content -- treat it as total loss of this entry.
