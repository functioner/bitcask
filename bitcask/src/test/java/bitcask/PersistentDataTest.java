package bitcask;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentDataTest {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentDataTest.class);

    static class Checker {
        final ArrayList<DataFileEntry> entries = new ArrayList<>();
        int pos = 0;
        void check(final PersistentData.Entry incoming) {
            final DataFileEntry actual = incoming.entry;
            assertTrue(pos < entries.size());
            final DataFileEntry expected = entries.get(pos);
            pos++;
            assertEquals(actual.CRC, expected.CRC);
            assertEquals(actual.timestamp, expected.timestamp);
            assertEquals(actual.keySize, expected.keySize);
            assertEquals(actual.valueSize, expected.valueSize);
            assertEquals(actual.key, expected.key);
            assertEquals(actual.value, expected.value);
            assertArrayEquals(actual.data, expected.data);
        }
    }

    @RepeatedTest(5)
    void testWriteAndCrash(@TempDir Path tempDir) throws Exception {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final Path path = tempDir.resolve("" + random.nextInt(10));
        DataFileEntry[] entries = BitcaskTest.genEntries();
        try (final PersistentData data = new PersistentData(path, i->{})) {
            for (final DataFileEntry entry : entries) {
                data.append(entry);
            }
        }
        final Checker checker = new Checker();
        checker.entries.addAll(Arrays.asList(entries));
        try (final PersistentData data = new PersistentData(path, checker::check)) {
            assertEquals(checker.pos, checker.entries.size());
            entries = BitcaskTest.genEntries();
            for (final DataFileEntry entry : entries) {
                data.append(entry);
            }
            checker.entries.addAll(Arrays.asList(entries));
            while (true) {
                final DataFileEntry entry = BitcaskTest.genEntry(random);
                if (entry.data.length > PersistentData.APPEND_SIZE_LIMIT) {
                    data.appendAndCrash(entry);
                    break;
                }
                data.append(entry);
                checker.entries.add(entry);
            }
        }
        checker.pos = 0;
        try (final PersistentData data = new PersistentData(path, checker::check)) {
            assertEquals(checker.pos, checker.entries.size());
            entries = BitcaskTest.genEntries();
            for (final DataFileEntry entry : entries) {
                data.append(entry);
            }
            checker.entries.addAll(Arrays.asList(entries));
        }
        checker.pos = 0;
        try (final PersistentData data = new PersistentData(path, checker::check)) {
            assertEquals(checker.pos, checker.entries.size());
        }
    }

    @RepeatedTest(5)
    void testSequential(@TempDir Path tempDir) throws Exception {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final Path path = tempDir.resolve("" + random.nextInt(10));
        DataFileEntry[] entries = BitcaskTest.genEntries();
        final Map<Integer, KeyDir.Value> keyDir = new HashMap<>();
        final Map<Integer, String> map = new HashMap<>();
        try (final PersistentData data = new PersistentData(path, i->{})) {
            for (final DataFileEntry entry : entries) {
                final KeyDir.Value value = data.append(entry);
                keyDir.put(entry.key, value);
                map.put(entry.key, entry.value);
            }
            final Object[] set = keyDir.entrySet().toArray();
            for (int n = random.nextInt(1000) + 1000; n > 0; n--) {
                final Map.Entry<Integer, KeyDir.Value> entry = (Map.Entry<Integer, KeyDir.Value>) set[random.nextInt(set.length)];
                final KeyDir.Value value = entry.getValue();
                assertEquals(data.read(value.fileId, value.valuePos, value.valueSize), map.get(entry.getKey()));
            }
        }
    }

    @RepeatedTest(5)
    void testConcurrent(@TempDir Path tempDir) throws Exception {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final Path path = tempDir.resolve("" + random.nextInt(10));
        DataFileEntry[] entries = BitcaskTest.genEntries();
        final Map<Integer, KeyDir.Value> keyDir = new HashMap<>();
        final Map<Integer, String> map = new HashMap<>();
        try (final PersistentData data = new PersistentData(path, i->{})) {
            for (final DataFileEntry entry : entries) {
                final KeyDir.Value value = data.append(entry);
                keyDir.put(entry.key, value);
                map.put(entry.key, entry.value);
            }
            final Object[] set = keyDir.entrySet().toArray();
            final Thread[] threads = new Thread[random.nextInt(10) + 10];
            final AtomicBoolean flag = new AtomicBoolean(true);
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        for (int n = random.nextInt(1000) + 1000; n > 0; n--) {
                            final Map.Entry<Integer, KeyDir.Value> entry = (Map.Entry<Integer, KeyDir.Value>) set[random.nextInt(set.length)];
                            final KeyDir.Value value = entry.getValue();
                            try {
                                assertEquals(data.read(value.fileId, value.valuePos, value.valueSize), map.get(entry.getKey()));
                            } catch (Exception e) {
                                fail(e);
                            }
                        }
                    } catch (final Throwable t) {
                        flag.set(false);
                        throw t;
                    }
                });
            }
            for (final Thread thread : threads) thread.start();
            for (final Thread thread : threads) thread.join();
            assertTrue(flag.get());
        }
    }
}
