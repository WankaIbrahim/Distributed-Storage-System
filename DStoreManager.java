import java.net.Socket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DStoreManager {
    private final ConcurrentHashMap<Integer, AbstractMap.SimpleEntry<Integer, DStoreConnection>> connectionMap = new ConcurrentHashMap<>();

    public void addDStore(Integer port, DStoreConnection connection, Integer space_used) {
        connectionMap.put(port, new AbstractMap.SimpleEntry<>(space_used, connection));
    }

    public void removeDStore(Socket socket) {
        connectionMap.entrySet().removeIf(entry -> {
            DStoreConnection dstoreConnection = entry.getValue().getValue();
            return dstoreConnection.socket() == socket;
        });
    }

    public DStoreConnection getDStore(Integer port) {
        return connectionMap.get(port).getValue();
    }

    public Integer getPort(Socket socket) {
        return connectionMap.entrySet()
                .stream()
                .filter(e -> e.getValue().getValue().socket().equals(socket))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    public void updateSpace(Integer port, Integer space_used) {
        connectionMap.computeIfPresent(port, (key, oldEntry) ->
                new AbstractMap.SimpleEntry<>(oldEntry.getKey() + space_used, oldEntry.getValue())
        );
    }

    public void clearSpace(Integer port) {
        connectionMap.computeIfPresent(port, (key, oldEntry) ->
                new AbstractMap.SimpleEntry<>(0, oldEntry.getValue())
        );
    }

    public int getNumberOfConnections() {
        return connectionMap.size();
    }

    public ArrayList<DStoreConnection> getConnections() {
        ArrayList<DStoreConnection> connections = new ArrayList<>();
        for (AbstractMap.SimpleEntry<Integer, DStoreConnection> entry : connectionMap.values()) {
            connections.add(entry.getValue());
        }
        return connections;
    }

    public ArrayList<Integer> getNDStores(int n) {
        return (ArrayList<Integer>) connectionMap.entrySet()
                .stream()
                .sorted(Comparator
                        .comparingInt((Map.Entry<Integer, AbstractMap.SimpleEntry<Integer, DStoreConnection>> entry)
                                -> entry.getValue().getKey())
                        .thenComparing(Map.Entry::getKey))
                .limit(n)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public ArrayList<Integer> getDStores() {
        ArrayList<Integer> list = new ArrayList<>();
        for (DStoreConnection dstoreConnection : getConnections()) {
            list.add(getPort(dstoreConnection.socket()));
        }
        return list;
    }
}