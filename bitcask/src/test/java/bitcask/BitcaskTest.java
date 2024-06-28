package bitcask;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BitcaskTest {
    private static final Logger LOG = LoggerFactory.getLogger(BitcaskTest.class);

    private static final String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    public static DataFileEntry genEntry(final Random random) {
        final StringBuilder salt = new StringBuilder();
        final int len = random.nextInt(4097);
        for (int j = 0; j < len; j++) salt.append(SALTCHARS.charAt(random.nextInt(SALTCHARS.length())));
        return new DataFileEntry(System.currentTimeMillis(), random.nextInt(), salt.toString());
    }

    public static DataFileEntry[] genEntries() {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final int n = random.nextInt(1000) + 1000;
        final DataFileEntry[] entries = new DataFileEntry[n];
        for (int i = 0; i < n; i++) {
            entries[i] = genEntry(random);
        }
        return entries;
    }

    @RepeatedTest(1)
    void clientServerTest(@TempDir Path tempDir) throws Exception {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        final Path path = tempDir.resolve("" + random.nextInt(10));
        final String name = "test";
        final AtomicBoolean flag = new AtomicBoolean(true);
        try (final KVStoreStub stub = new KVStoreStub(name, 1099, path)) {
            final Thread[] threads = new Thread[random.nextInt(5) + 5];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        final KVStoreRemote client = KVStoreStub.getClient(name);
                        final ArrayList<DataFileEntry> entries = new ArrayList<>();
                        for (int j = 0; j < 1000; j++) {
                            if (!entries.isEmpty() && random.nextBoolean()) {
                                final int key = entries.get(random.nextInt(entries.size())).key;
                                if (random.nextBoolean()) {
                                    client.get(key);
                                } else {
                                    client.put(key, genEntry(random).value);
                                }
                            } else {
                                final DataFileEntry entry = genEntry(random);
                                if (random.nextBoolean()) {
                                    client.get(entry.key);
                                } else {
                                    client.put(entry.key, entry.value);
                                }
                            }
                        }
                    } catch (Exception e) {
                        flag.set(false);
                        throw new RuntimeException(e);
                    }
                });
            }
        }
        assertTrue(flag.get());
    }
}
