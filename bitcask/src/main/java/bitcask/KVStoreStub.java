package bitcask;

import java.io.IOException;
import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class KVStoreStub implements AutoCloseable {
    private final Bitcask bitcask;
    private final KVStoreRemote stub;

    public KVStoreStub(final String name, final int port, final Path path) throws IOException {
        this.bitcask = new Bitcask(path);
        this.stub = (KVStoreRemote) UnicastRemoteObject.exportObject((KVStoreRemote) this.bitcask, 0);
        final Registry registry = LocateRegistry.createRegistry(port);
        registry.rebind(name, stub);
    }

    public static KVStoreRemote getClient(final String name) throws RemoteException, NotBoundException {
        Registry registry = LocateRegistry.getRegistry();
        return (KVStoreRemote) registry.lookup(name);
    }

    @Override
    public void close() throws Exception {
        this.bitcask.close();
    }
}
