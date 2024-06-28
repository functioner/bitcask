package bitcask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.function.Consumer;

public final class PersistentData implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentData.class);
    public static final int APPEND_SIZE_LIMIT = 4 * 1024;

    public final ArrayList<FileReader> dataFiles = new ArrayList<>();
    public final Path path;

    public volatile int currentFileId = 0;
    public volatile long currentFileBytes = 0;
    public volatile FileOutputStream currentDataFile = null;

    public static final class Entry {
        public final DataFileEntry entry;
        public final int fileId;
        public final long offset;
        public Entry(final DataFileEntry entry, final int fileId, final long offset) {
            this.entry = entry;
            this.fileId = fileId;
            this.offset = offset;
        }
    }

    public PersistentData(final Path path, final Consumer<Entry> loader) throws IOException {
        this.path = path;
        Files.createDirectories(path);
        File current = path.resolve(Bitcask.DATA_FILE_PREFIX + currentFileId).toFile();
        while (current.exists()) {
            currentFileBytes = 0;
            try (final FileInputStream input = new FileInputStream(current)) {
                while (true) {
                    try {
                        final DataFileEntry entry = new DataFileEntry(input);
                        final long offset = currentFileBytes + DataFileEntry.VALUE_OFFSET;
                        currentFileBytes += entry.data.length;
                        loader.accept(new Entry(entry, currentFileId, offset));
                    } catch (final DataFileEntry.EOF e) {
                        break;
                    }
                }
            }
            if (current.length() > currentFileBytes) {
                try (final RandomAccessFile file = new RandomAccessFile(current, "rw")) {
                    file.getChannel().truncate(currentFileBytes);
                }
            }
            dataFiles.add(new FileReader(current));
            currentFileId++;
            current = path.resolve(Bitcask.DATA_FILE_PREFIX + currentFileId).toFile();
        }
        if (currentFileBytes < Bitcask.DATA_FILE_SIZE_LIMIT) {
            if (currentFileId > 0) {
                currentFileId--;
                currentDataFile = new FileOutputStream(path.resolve(Bitcask.DATA_FILE_PREFIX + currentFileId).toFile(), true);
            }
        } else {
            currentFileBytes = 0;
        }
    }

    public String read(final int fileId, final long pos, final short size) throws IOException, InterruptedException {
        final FileReader reader;
        // there will not be too many files, so it is not bottleneck
        synchronized (dataFiles) {
            reader = dataFiles.get(fileId);
        }
        return reader.read(pos, size);
    }

    private synchronized KeyDir.Value append(final DataFileEntry entry, final boolean crash) throws IOException {
        if (currentDataFile == null) {
            final File current = path.resolve(Bitcask.DATA_FILE_PREFIX + currentFileId).toFile();
            currentDataFile = new FileOutputStream(current);
            synchronized (dataFiles) {
                dataFiles.add(new FileReader(current));
            }
        }
        final KeyDir.Value result = new KeyDir.Value(currentFileId, entry.valueSize, currentFileBytes + DataFileEntry.VALUE_OFFSET, entry.timestamp);
        if (entry.data.length > APPEND_SIZE_LIMIT) {
            currentDataFile.write(entry.data, 0, APPEND_SIZE_LIMIT);
            currentDataFile.flush();
            if (crash) return null;
            currentDataFile.write(entry.data, APPEND_SIZE_LIMIT, entry.data.length - APPEND_SIZE_LIMIT);
            currentDataFile.flush();
        } else {
            currentDataFile.write(entry.data);
            currentDataFile.flush();
        }
        currentFileBytes += entry.data.length;
        if (currentFileBytes > Bitcask.DATA_FILE_SIZE_LIMIT) {
            currentFileBytes = 0;
            currentFileId++;
            currentDataFile.close();
            currentDataFile = null;
        }
        return result;
    }

    public KeyDir.Value append(final DataFileEntry entry) throws IOException {
        return append(entry, false);
    }

    public void appendAndCrash(final DataFileEntry entry) throws IOException {
        if (append(entry, true) != null) {
            throw new IOException("Expect to crash but no crash");
        }
    }

    @Override
    public void close() throws Exception {
        if (currentDataFile != null) {
            currentDataFile.close();
        }
        for (final FileReader reader : dataFiles) reader.close();
    }
}
