package bitcask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public final class FileReader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

    public static final int CONCURRENT_READERS = 5;

    private static final class Read {
        private String result = null;
        private final long pos;
        private final short size;
        private IOException error = null;
        private CountDownLatch latch = new CountDownLatch(1);

        private Read(final long pos, final short size) {
            this.pos = pos;
            this.size = size;
        }
    }

    private final RandomAccessFile[] files;
    private final Thread[] readers;
    private final LinkedBlockingQueue<Read> reads;

    public FileReader(final File file) throws IOException {
        this.files = new RandomAccessFile[CONCURRENT_READERS];
        this.readers = new Thread[CONCURRENT_READERS];
        this.reads = new LinkedBlockingQueue<>();
        for (int i = 0; i < this.files.length; i++) {
            final RandomAccessFile fileReader = new RandomAccessFile(file, "r");
            this.files[i] = fileReader;
            this.readers[i] = new Thread(() -> {
                while (true) {
                    Read read = null;
                    try {
                        read = reads.take();
                    } catch (final InterruptedException e) {
                        return;
                    }
                    try {
                        fileReader.seek(read.pos);
                        final byte[] result = new byte[read.size];
                        fileReader.readFully(result);
                        read.result = new String(result);
                    } catch (final IOException e) {
                        read.error = e;
                    }
                    read.latch.countDown();
                }
            });
        }
        for (final Thread reader : readers) reader.start();
    }

    public String read(final long pos, final short size) throws IOException, InterruptedException {
        if (size == -1) return null;
        final Read read = new Read(pos, size);
        this.reads.put(read);
        read.latch.await();
        if (read.error == null) return read.result;
        throw read.error;
    }

    @Override
    public void close() throws Exception {
        for (final Thread reader : this.readers) {
            reader.interrupt();
        }
        for (final Thread reader : this.readers) {
            reader.join();
        }
        for (final RandomAccessFile file : this.files) {
            file.close();
        }
    }
}
