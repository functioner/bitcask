package bitcask;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface KVStoreRemote extends Remote {
    String get(final int key) throws RemoteException;
    void put(final int key, final String value) throws RemoteException;
}
