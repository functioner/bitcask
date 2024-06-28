package bitcask;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataFileEntryTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataFileEntryTest.class);

    @RepeatedTest(5)
    void fileTest(@TempDir Path tempDir) throws IOException {
        final File file = tempDir.resolve(Bitcask.DATA_FILE_PREFIX + 0).toFile();
        assertTrue(file.createNewFile());
        final DataFileEntry[] entries = BitcaskTest.genEntries();
        try (final FileOutputStream output = new FileOutputStream(file)) {
            for (final DataFileEntry entry : entries) {
                output.write(entry.data);
            }
        }
        final ArrayList<DataFileEntry> list = new ArrayList<>();
        try (final FileInputStream input = new FileInputStream(file)) {
            while (true) {
                try {
                    final DataFileEntry entry = new DataFileEntry(input);
                    list.add(entry);
                } catch (final DataFileEntry.EOF e) {
                    break;
                }
            }
        }
    }
}
