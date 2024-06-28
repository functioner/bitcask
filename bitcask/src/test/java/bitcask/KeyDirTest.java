package bitcask;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class KeyDirTest {
    private static final Logger LOG = LoggerFactory.getLogger(KeyDirTest.class);

    @RepeatedTest(5)
    void testSequential(@TempDir Path tempDir) throws Exception {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final Path path = tempDir.resolve("" + random.nextInt(10));
        final Map<Integer, String> map = new HashMap<>();
        final ArrayList<Integer> keys = new ArrayList<>();
        keys.add(random.nextInt());
        try (final KeyDir keyDir = new KeyDir(path)) {
            for (int n = random.nextInt(10000) + 10000; n > 0; n--) {
                int key;
                if (random.nextBoolean()) {
                    // new key
                    key = random.nextInt();
                    while (map.containsKey(key)) key = random.nextInt();
                    keys.add(key);
                } else {
                    // old key
                    key = keys.get(random.nextInt(keys.size()));
                }
                if (random.nextBoolean()) {
                    // write
                    final DataFileEntry entry = BitcaskTest.genEntry(random);
                    if (random.nextInt(10) == 0) {
                        // delete
                        keyDir.put(entry.key, null);
                    } else {
                        // update
                        keyDir.put(entry.key, entry.value);
                    }
                } else {
                    // read
                    assertEquals(map.get(key), keyDir.get(key));
                }
            }
        }
    }

    private static int nextInt(final Random random, final int bits, final int remainder) {
        return ((random.nextInt()) & ~((1<<bits)-1)) | remainder;
    }

    @RepeatedTest(5)
    void testConcurrent(@TempDir Path tempDir) throws Exception {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final Path path = tempDir.resolve("" + random.nextInt(10));
        final ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<>();
        final AtomicBoolean flag = new AtomicBoolean(true);
        try (final KeyDir keyDir = new KeyDir(path)) {
            final int bits = 4;
            final Thread[] threads = new Thread[1 << bits];
            for (int i = 0; i < threads.length; i++) {
                final Integer remainder = i;
                threads[i] = new Thread(() -> {
                    try {
                        final ArrayList<Integer> keys = new ArrayList<>();
                        keys.add(nextInt(random, bits, remainder));
                        for (int n = random.nextInt(1000) + 1000; n > 0; n--) {
                            int key;
                            if (random.nextBoolean()) {
                                // new key
                                key = nextInt(random, bits, remainder);
                                while (map.containsKey(key)) key = nextInt(random, bits, remainder);
                                keys.add(key);
                            } else {
                                // old key
                                key = keys.get(random.nextInt(keys.size()));
                            }
                            if (random.nextInt(4) == 0) {
                                // write
                                final DataFileEntry entry = BitcaskTest.genEntry(random);
                                if (random.nextInt(10) == 0) {
                                    // delete
                                    keyDir.put(entry.key, null);
                                } else {
                                    // update
                                    keyDir.put(entry.key, entry.value);
                                }
                            } else {
                                // read
                                try {
                                    assertEquals(map.get(key), keyDir.get(key));
                                } catch (final Exception e) {
                                    throw new RuntimeException(e);
                                }
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
        }
        assertTrue(flag.get());
    }
}
