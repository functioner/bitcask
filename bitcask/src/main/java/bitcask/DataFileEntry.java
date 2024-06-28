package bitcask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class DataFileEntry {
    private static final Logger LOG = LoggerFactory.getLogger(DataFileEntry.class);

    public final static int VALUE_OFFSET = 1 + 8 + 1 + 2 + 4;

    public final byte CRC;
    public final long timestamp;
    public final byte keySize;
    public final short valueSize;  // -1 when value is null
    public final int key;
    public final String value;  // delete when value is null
    public final byte[] data;

    public DataFileEntry(final long timestamp, final int key, final String value) {
        this.timestamp = timestamp;
        this.key = key;
        this.keySize = 4;
        this.value = value;
        if (value == null) {
            this.valueSize = -1;
        } else {
            this.valueSize = (short) value.length();
        }
        final ByteBuffer buffer = ByteBuffer.allocate(VALUE_OFFSET + Math.max(this.valueSize, 0));
        buffer.put((byte) 0);
        buffer.putLong(timestamp);
        buffer.put(keySize);
        buffer.putShort(valueSize);
        buffer.putInt(key);
        if (value != null) {
            buffer.put(value.getBytes(StandardCharsets.UTF_8));
        }
        data = buffer.array();
        byte CRC = 0;
        for (int i = 1; i < data.length; i++) CRC ^= data[i];
        data[0] = CRC;
        this.CRC = CRC;
    }

    public static final class EOF extends Exception {}

    public DataFileEntry(final FileInputStream input) throws EOF, IOException {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final byte[] timestamp = new byte[8];
        final byte[] key = new byte[4];
        final byte[] valueSize = new byte[2];

        int result = input.read();
        if (result == -1) throw new EOF();  // handle no more input
        final byte CRC = (byte) result;
        buffer.write(CRC);

        result = input.read(timestamp);
        if (result < timestamp.length) throw new IOException();
        this.timestamp = ByteBuffer.wrap(timestamp).getLong();
        buffer.write(timestamp);

        result = input.read();
        if (result == -1) throw new IOException();
        this.keySize = (byte) result;
        buffer.write(this.keySize);

        result = input.read(valueSize);
        if (result == -1) throw new IOException();
        this.valueSize = ByteBuffer.wrap(valueSize).getShort();
        buffer.write(valueSize);

        result = input.read(key);
        if (result < key.length) throw new IOException();
        this.key = ByteBuffer.wrap(key).getInt();
        buffer.write(key);

        if (this.valueSize == -1) {
            this.value = null;
        } else {
            final byte[] value = new byte[this.valueSize];
            result = input.read(value);
            if (result < (int) this.valueSize) throw new EOF();  // handle failed separated value chunks
            this.value = new String(value);
            buffer.write(value);
        }

        this.data = buffer.toByteArray();
        byte crc = 0;
        for (int i = 1; i < data.length; i++) crc ^= data[i];
        if (crc != CRC) throw new IOException("invalid data file");
        this.CRC = CRC;
    }
}
