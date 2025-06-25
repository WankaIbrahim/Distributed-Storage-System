import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class Controller {
    private static final Logger logger = Logger.getLogger(Controller.class.getName());
    private final DStoreManager dsm = new DStoreManager();
    private final Index index = new Index();
    private final ConcurrentHashMap<Integer, List<String>> fileMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PrintWriter> clientWriters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<ScheduledFuture<?>>> storeTimeouts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<ScheduledFuture<?>>> removeTimeouts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicBoolean> completed = new ConcurrentHashMap<>();
    private final AtomicInteger listCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private final AtomicInteger pending = new AtomicInteger(0);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService rebalanceScheduler = Executors.newSingleThreadScheduledExecutor();
    int cport;
    int repFactor;
    private final ScheduledExecutorService timeoutScheduler;
    int timeout;
    int rebalance_period;

    public Controller(int cport, int repFactor, int timeout, int rebalance_period) {
        this.cport = cport;
        this.repFactor = repFactor;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        this.timeoutScheduler = Executors.newScheduledThreadPool(repFactor);

        rebalanceScheduler.scheduleWithFixedDelay(() -> {
            if (pending.get() == 0) {
                rebalance();
            }
        }, rebalance_period, rebalance_period, TimeUnit.MILLISECONDS);

        startController(cport);
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: Controller <cport> <repFactor> <timeout> <rebalance_period>");
            System.exit(1);
        }
        int cport = Integer.parseInt(args[0]);
        int repFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        new Controller(cport, repFactor, timeout, rebalancePeriod);
    }

    private void startController(int cport) {
        try (ServerSocket controllerSocket = new ServerSocket(cport)) {
            logger.info("Starting controller on port " + controllerSocket.getLocalPort());
            while (true) {
                Socket DStoreSocket = controllerSocket.accept();
                new Thread(() -> handleConnection(DStoreSocket)).start();
            }
        } catch (IOException e) {
            logger.warning("Controller error: " + e.getMessage());
        }
    }

    private void handleConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {

            while (true) {
                String msg = in.readLine();
                if (msg == null) {
                    lock.writeLock().lock();
                    try {
                        handleDstoreFailure(socket);
                    } finally {
                        lock.writeLock().unlock();
                    }
                    break;
                }

                String cmd = msg.split(" ")[0];
                System.out.println("Received message: " + msg);

                switch (cmd) {
                    case Protocol.JOIN_TOKEN, Protocol.STORE_TOKEN, Protocol.STORE_ACK_TOKEN,
                         Protocol.REMOVE_TOKEN, Protocol.REMOVE_ACK_TOKEN,
                         Protocol.REBALANCE_COMPLETE_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
                        lock.writeLock().lock();
                        try {
                            switch (cmd) {
                                case Protocol.JOIN_TOKEN -> join(msg, socket, out, in);
                                case Protocol.STORE_TOKEN -> store(msg, out);
                                case Protocol.STORE_ACK_TOKEN -> storeAck(msg);
                                case Protocol.REMOVE_TOKEN -> remove(msg, out);
                                case Protocol.REMOVE_ACK_TOKEN -> removeAck(msg);
                                case Protocol.REBALANCE_COMPLETE_TOKEN -> rebalanceComplete();
                                case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> error(msg);
                            }
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }

                    case Protocol.LIST_TOKEN -> list(msg, socket, out);

                    case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN -> {
                        lock.readLock().lock();
                        try {
                            if (cmd.equals(Protocol.LOAD_TOKEN)) load(msg, out);
                            else reload(msg, out);
                        } finally {
                            lock.readLock().unlock();
                        }
                    }

                    default -> System.out.println("Unknown command: " + msg);
                }
            }
        } catch (IOException e) {
            logger.info("Controller error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ex) {
                logger.warning("Controller error: " + ex.getMessage());
            }
        }
    }

    private void join(String message, Socket socket, PrintWriter out, BufferedReader in) {
        DStoreConnection dsc = new DStoreConnection(socket, out, in);
        int port = Integer.parseInt(message.split(" ")[1]);
        dsm.addDStore(port, dsc, 0);
        System.out.println("DStore: " + port + " has joined the party");
        if (dsm.getNumberOfConnections() >= repFactor) {
            rebalanceScheduler.submit(this::rebalance);
        }
    }

    private void store(String message, PrintWriter out) {
        String filename = message.split(" ")[1];
        int filesize = Integer.parseInt(message.split(" ")[2]);

        if (dsm.getNumberOfConnections() < repFactor) {
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        } else if (index.exists(filename)) {
            out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }

        ArrayList<Integer> ports = dsm.getNDStores(repFactor);
        counters.put(filename, new AtomicInteger(ports.size()));
        pending.addAndGet(ports.size());
        completed.put(filename, new AtomicBoolean(false));
        System.out.println("Store started. Pending = " + pending);

        StringBuilder sb = new StringBuilder(Protocol.STORE_TO_TOKEN);
        for (int port : ports) {
            sb.append(" ").append(port);
            dsm.updateSpace(port, filesize);
        }

        index.addEntry(new IndexEntry(new FileData(filename, filesize), ports, 1));
        out.println(sb);
        clientWriters.put(filename, out);

        ScheduledFuture<?> timeoutFuture = timeoutScheduler.schedule(() -> {
            AtomicBoolean done = completed.get(filename);
            if (done != null && !done.get()) {
                AtomicInteger counter = counters.remove(filename);
                if (counter != null && counter.get() > 0) {
                    index.removeEntry(filename);
                    ports.forEach(p -> dsm.updateSpace(p, -filesize));
                    pending.addAndGet(-counter.get());
                    System.out.println("Store timed out. Pending = " + pending);
                    clientWriters.remove(filename);
                }
            }
        }, timeout, TimeUnit.MILLISECONDS);
        storeTimeouts.computeIfAbsent(filename, k -> new CopyOnWriteArrayList<>()).add(timeoutFuture);
    }

    private void storeAck(String message) {
        String filename = message.split(" ")[1];
        AtomicInteger counter = counters.get(filename);
        if (counter == null) {
            logger.warning("Unexpected STORE_ACK for " + filename);
            return;
        }
        int remaining = counter.decrementAndGet();
        pending.decrementAndGet();

        if (remaining == 0) {
            storeTimeouts.remove(filename).forEach(f -> f.cancel(true));
            counters.remove(filename);
            index.setState(filename, 2);
            completed.get(filename).set(true);
            PrintWriter clientOut = clientWriters.get(filename);
            if (clientOut != null) {
                clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                System.out.println("Store of file " + filename + " complete.");
            } else {
                logger.warning("Client writer not found for file: " + filename);
            }
        }
    }

    private void load(String message, PrintWriter out) {
        String path = message.split(" ")[1];
        String filename = path.substring(path.lastIndexOf("\\") + 1);

        if (dsm.getNumberOfConnections() < repFactor) {
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        } else if (!index.exists(filename) || index.getState(filename) != 2) {
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        IndexEntry entry = index.getEntry(filename);
        if (message.split(" ")[0].equals(Protocol.LOAD_TOKEN)) {
            counters.put(filename, new AtomicInteger(entry.getLocations().size()));
        }

        if (counters.get(filename).get() == 0) {
            out.println(Protocol.ERROR_LOAD_TOKEN);
            return;
        }

        int port = entry.getLocations().get(counters.get(filename).decrementAndGet());
        int filesize = entry.getFile().filesize();
        out.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + filesize);
        System.out.println("Load of file " + filename + " initialised");
    }

    private void reload(String message, PrintWriter out) {
        load(message, out);
    }

    private void remove(String message, PrintWriter out) {
        String path = message.split(" ")[1];
        String filename = path.substring(path.lastIndexOf("\\") + 1);
        if (dsm.getNumberOfConnections() < repFactor) {
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        } else if (!index.exists(filename) || index.getState(filename) != 2) {
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        List<Integer> original = index.getEntry(filename).getLocations();
        Set<Integer> liveAtSnapshot = new HashSet<>(dsm.getDStores());
        List<Integer> toSend = original.stream()
                .filter(liveAtSnapshot::contains)
                .toList();
        List<Integer> missingBefore = original.stream()
                .filter(p -> !liveAtSnapshot.contains(p))
                .toList();

        index.setState(filename, 3);

        counters.put(filename, new AtomicInteger(original.size()));
        pending.addAndGet(original.size());
        completed.get(filename).set(false);
        System.out.println("Remove started. Pending = " + pending);

        for (int port : toSend) {
            dsm.getDStore(port).out().println(Protocol.REMOVE_TOKEN + " " + filename);
            dsm.updateSpace(port, -index.getEntry(filename).getFile().filesize());
        }

        clientWriters.put(filename, out);

        for (int ignored : missingBefore) {
            removeAck(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        }

        ScheduledFuture<?> timeoutFuture = timeoutScheduler.schedule(() -> {
            AtomicInteger counter = counters.remove(filename);
            if (counter != null && counter.get() > 0) {
                pending.addAndGet(-counter.get());
                System.out.println("Remove timed out. Pending = " + pending);
                clientWriters.remove(filename);
            }
        }, timeout, TimeUnit.MILLISECONDS);
        removeTimeouts.computeIfAbsent(filename, k -> new CopyOnWriteArrayList<>())
                .add(timeoutFuture);
    }

    private void removeAck(String message) {
        String filename = message.split(" ")[1];
        AtomicInteger counter = counters.get(filename);
        if (counter == null) {
            logger.warning("Unexpected REMOVE_ACK for " + filename);
            return;
        }
        int remaining = counter.decrementAndGet();
        pending.decrementAndGet();

        System.out.println("REMOVE_ACK for " + filename + " remaining. = " + remaining);

        if (remaining == 0) {
            removeTimeouts.remove(filename).forEach(f -> f.cancel(true));
            counters.remove(filename);
            index.removeEntry(filename);
            completed.get(filename).set(true);
            PrintWriter clientOut = clientWriters.get(filename);
            if (clientOut != null) {
                clientOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                System.out.println("Remove of file " + filename + " complete.");
            } else {
                logger.warning("Client writer not found for file: " + filename);
            }
        }
    }

    private void error(String message) {
        removeAck(message);
    }

    private void list(String message, Socket socket, PrintWriter out) {
        Integer port = dsm.getPort(socket);
        if (port != null) {
            List<String> fileList = stream(message.split(" "))
                    .skip(1)
                    .collect(toList());
            fileMap.put(port, fileList);
            listCounter.decrementAndGet();
        } else {
            if (dsm.getNumberOfConnections() < repFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
            out.println(Protocol.LIST_TOKEN + index.list());
        }
    }

    private void rebalance() {
        if (pending.get() > 0) return;
        try {
            if (dsm.getNumberOfConnections() < repFactor) return;
            System.out.println("Starting Rebalance");

            long deadline = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < deadline) {
                lock.readLock().lock();
                try {
                    if (index.ready()) break;
                } finally {
                    lock.readLock().unlock();
                }
                Thread.sleep(50);
            }

            lock.writeLock().lock();
            try {
                fileMap.clear();
                listCounter.set(dsm.getNumberOfConnections());
            } finally {
                lock.writeLock().unlock();
            }

            dsm.getConnections().forEach(dsc -> dsc.out().println(Protocol.LIST_TOKEN));

            long waitEnd = System.currentTimeMillis() + timeout;
            while (listCounter.get() > 0 && System.currentTimeMillis() < waitEnd) {
                Thread.yield();
            }
            if (listCounter.get() > 0) {
                System.out.println("LISTs not received in time");
                return;
            }

            lock.writeLock().lock();
            try {
                rebalanceIndex();
                Map<Integer, String> sending = getSendingDStores();
                Map<Integer, String> removing = getRemovingDStores();

                for (int port : dsm.getDStores()) {
                    if (sending.containsKey(port) || removing.containsKey(port)) {
                        String msg = sending.getOrDefault(port, "0")
                                + " "
                                + removing.getOrDefault(port, "0");
                        pending.incrementAndGet();
                        dsm.getDStore(port).out().println(Protocol.REBALANCE_TOKEN + " " + msg);
                    }
                }
                System.out.println("Rebalance completed");
            } finally {
                lock.writeLock().unlock();
            }
        } catch (Throwable e) {
            logger.warning(e.getMessage());
        }
    }

    private void rebalanceIndex() {
        HashMap<String, Integer> fileData = new HashMap<>();
        ArrayList<String> filesToRemove = new ArrayList<>();
        for (IndexEntry file : index.getFiles()) {
            if (file.getState() == 3) filesToRemove.add(file.getFile().filename());
            fileData.put(file.getFile().filename(), file.getFile().filesize());
        }

        Set<String> filesInDstores = new HashSet<>();
        for (List<String> list : fileMap.values()) filesInDstores.addAll(list);

        ArrayList<String> lostFiles = new ArrayList<>();
        for (String file : fileData.keySet()) {
            if (!filesInDstores.contains(file)) {
                logger.info(file + " has been lost.");
                lostFiles.add(file);
            }
        }
        for (String file : lostFiles) fileData.remove(file);


        index.getFiles().clear();
        for (Integer port : dsm.getDStores()) {
            dsm.clearSpace(port);
        }


        for (Map.Entry<String, Integer> file : fileData.entrySet()) {
            String filename = file.getKey();
            Integer filesize = file.getValue();
            ArrayList<Integer> ports = dsm.getNDStores(repFactor);
            for (Integer port : ports) {
                dsm.updateSpace(port, filesize);
            }
            if (filesToRemove.contains(filename)) {
                index.addEntry(new IndexEntry(new FileData(filename, filesize), ports, 3));
            } else {
                index.addEntry(new IndexEntry(new FileData(filename, filesize), ports, 2));
            }

        }
    }

    private HashMap<Integer, String> getSendingDStores() {
        HashMap<Integer, String> sendingDStores = new HashMap<>();
        HashMap<Integer, Integer> portCount = new HashMap<>();

        for (IndexEntry file : index.getFiles()) {
            String fileName = file.getFile().filename();
            Integer sendingDstore = 0;
            int noOfDStoresReceiving = 0;

            for (Map.Entry<Integer, List<String>> entry : fileMap.entrySet()) {
                if (entry.getValue().contains(fileName)) {
                    sendingDstore = entry.getKey();
                    break;
                }
            }

            StringBuilder receivingDStores = new StringBuilder();
            for (Integer port : file.getLocations()) {
                List<String> storedAtPort = fileMap.getOrDefault(port, Collections.emptyList());
                if (!storedAtPort.contains(fileName)) {
                    receivingDStores.append(" ").append(port);
                    noOfDStoresReceiving++;
                    dsm.updateSpace(port, file.getFile().filesize());
                }
            }

            if (noOfDStoresReceiving > 0) {
                String sendMsg = fileName + " " + noOfDStoresReceiving + receivingDStores;
                if (sendingDStores.containsKey(sendingDstore)) {
                    int count = portCount.get(sendingDstore) + 1;
                    portCount.put(sendingDstore, count);
                    sendMsg = sendingDStores.get(sendingDstore) + " " + sendMsg;
                    sendMsg = sendMsg.replaceFirst("\\d+", String.valueOf(count));
                } else {
                    portCount.put(sendingDstore, 1);
                    sendMsg = "1 " + sendMsg;
                }
                sendingDStores.put(sendingDstore, sendMsg);
            }
        }
        return sendingDStores;
    }

    private HashMap<Integer, String> getRemovingDStores() {
        HashMap<Integer, String> removingDStores = new HashMap<>();

        for (Integer dstore : dsm.getDStores()) {
            int filesToRemove = 0;
            StringBuilder sb = new StringBuilder();

            List<String> storedFiles = fileMap.getOrDefault(dstore, Collections.emptyList());
            for (String filename : storedFiles) {
                IndexEntry entry = index.getEntry(filename);
                if (entry == null) {
                    sb.append(" ").append(filename);
                    filesToRemove++;
                } else if (!entry.getLocations().contains(dstore) || entry.getState() == 3) {
                    sb.append(" ").append(filename);
                    filesToRemove++;
                    dsm.updateSpace(dstore, -entry.getFile().filesize());
                }
            }
            if (filesToRemove > 0) {
                removingDStores.put(dstore, filesToRemove + sb.toString());
            }
        }
        return removingDStores;
    }

    private void rebalanceComplete() {
        pending.decrementAndGet();
    }

    private void handleDstoreFailure(Socket socket) {
        Integer failedPort = dsm.getPort(socket);
        System.out.println("Connection dropped: " + failedPort);
        if (failedPort != null) {
            dsm.removeDStore(socket);

            for (IndexEntry entry : index.getFiles()) {
                String fn = entry.getFile().filename();
                ArrayList<Integer> locs = entry.getLocations();

                if (index.getState(fn) == 3) {
                    removeAck(Protocol.REMOVE_ACK_TOKEN + " " + fn);
                } else {
                    locs.remove(failedPort);
                    entry.setLocations(locs);
                    index.updateEntry(entry);
                }
            }
        }
    }
}