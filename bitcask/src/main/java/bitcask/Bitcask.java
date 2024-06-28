package bitcask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.rmi.RemoteException;

public class Bitcask implements KVStoreRemote, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Bitcask.class);

    public static final String DATA_FILE_PREFIX = "data_file_";
    public static final int DATA_FILE_SIZE_LIMIT = 4 * 1024 * 100;

    private final KeyDir keyDir;

    public Bitcask(final Path path) throws IOException {
        this.keyDir = new KeyDir(path);
    }

    @Override
    public String get(int key) throws RemoteException {
        try {
            return keyDir.get(key);
        } catch (Exception e) {
            throw new RemoteException("KeyDir error", e);
        }
    }

    @Override
    public void put(int key, String value) throws RemoteException {
        keyDir.put(key, value);
    }

    @Override
    public void close() throws Exception {
        keyDir.close();
    }
}
