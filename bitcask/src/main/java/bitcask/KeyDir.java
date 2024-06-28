package bitcask;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public final class KeyDir implements AutoCloseable {
    public static final class Value {
        public final int fileId;
        public final short valueSize;
        public final long valuePos;
        public final long timestamp;
        public Value(final int fileId, final short valueSize, final long valuePos, final long timestamp) {
            this.fileId = fileId;
            this.valueSize = valueSize;
            this.valuePos = valuePos;
            this.timestamp = timestamp;
        }
    }

    private final ConcurrentHashMap<Integer, Value> map = new ConcurrentHashMap<>();
    private final PersistentData data;

    public KeyDir(final Path path) throws IOException {
        data = new PersistentData(path, entry -> {
            map.put(entry.entry.key, new Value(entry.fileId, entry.entry.valueSize, entry.offset, entry.entry.timestamp));
        });
    }

    public void put(final int key, final String value) {
        map.compute(key, (k, v) -> {
            try {
                return data.append(new DataFileEntry(System.currentTimeMillis(), key, value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public String get(final int key) throws IOException, InterruptedException {
        final Value value = map.get(key);
        if (value == null) return null;
        return data.read(value.fileId, value.valuePos, value.valueSize);
    }

    @Override
    public void close() throws Exception {
        data.close();
    }
}
