import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.logging.Logger;

public class Dstore {
    private static final Logger logger = Logger.getLogger(Dstore.class.getName());

    private final int port;
    private final int cport;
    private final int timeout;
    private final File file_folder;

    private PrintWriter controllerOutbound;

    public Dstore(int port, int cport, int timeout, String file_folder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = new File(file_folder);

        createStoreFolder();
        new Thread(this::startDStore).start();
        connectToController();
        sendControllerMessage(Protocol.JOIN_TOKEN + " " + port);
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: java Dstore <port> <cport> <timeout> <file_folder>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String folder = args[3];

        new Dstore(port, cport, timeout, folder);

//        new Dstore(1000, 12345, 5000, "dir1");
//        new Dstore(2000, 12345, 5000, "dir2");
//        new Dstore(3000, 12345, 5000, "dir3");
//        new Dstore(4000, 12345, 5000, "dir4");
//        new Dstore(5000, 12345, 5000, "dir5");
    }

    private void createStoreFolder() {
        for (File f : Objects.requireNonNull(file_folder.listFiles())) {
            f.delete();
        }
    }

    private void startDStore() {
        try (ServerSocket DStoreSocket = new ServerSocket(port)) {
            logger.info("Starting DStore on port " + DStoreSocket.getLocalPort());
            while (true) {
                Socket clientSocket = DStoreSocket.accept();
                new Thread(() -> handleConnection(clientSocket)).start();
            }
        } catch (IOException e) {
            logger.warning("DStore error: " + e.getMessage());
        }
    }

    private void connectToController() {
        while (true) {
            try {
                Socket controllerSocket = new Socket("localhost", cport);
                controllerOutbound = new PrintWriter(controllerSocket.getOutputStream(), true);
                BufferedReader controllerInbound =
                        new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

                new Thread(() -> {
                    String msg;
                    try {
                        while ((msg = controllerInbound.readLine()) != null) {
                            System.out.println(msg + " received on port " + port + " from controller.");
                            switch (msg.split(" ")[0]) {
                                case Protocol.REMOVE_TOKEN -> remove(msg);
                                case Protocol.LIST_TOKEN   -> list();
                                case Protocol.REBALANCE_TOKEN -> rebalance(msg);
                                default -> System.out.println("Unknown command: " + msg);
                            }
                        }
                    } catch (IOException e) {
                        logger.warning("Connection to controller lost: " + e.getMessage());
                    }
                }).start();
                return;

            } catch (IOException e) {
                logger.warning("Failed to connect to controller on port "
                        + cport + ": " + e.getMessage()
                        + " — retrying in 1s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry controller connect", ie);
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String msg;
            while ((msg = in.readLine()) != null) {
                switch (msg.split(" ")[0]) {
                    case Protocol.STORE_TOKEN -> store(msg, socket, out);
                    case Protocol.LOAD_DATA_TOKEN -> loadData(msg, socket);
                    case Protocol.REBALANCE_STORE_TOKEN -> rebalanceStore(msg, socket);
                    case Protocol.ACK_TOKEN -> rebalanceSend(msg, socket);
                    default -> System.out.println("Unknown command: " + msg);
                }
            }
        } catch (IOException e) {
            logger.info("Client socket closed: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warning("DStore error: " + e.getMessage());
            }
        }
    }

    private void sendControllerMessage(String message) {
        if (controllerOutbound != null) {
            controllerOutbound.println(message);
            controllerOutbound.flush();
            System.out.println("Sent to controller from port " + port + ": " + message);
        } else {
            logger.warning("Attempted to send message with null output stream");
        }
    }

    private void store(String message, Socket socket, PrintWriter out) {
        out.println(Protocol.ACK_TOKEN);

        String filename = message.split(" ")[1];
        int filesize = Integer.parseInt(message.split(" ")[2]);

        try (InputStream in = socket.getInputStream();
             FileOutputStream fos = new FileOutputStream(new File(file_folder, filename))) {
            byte[] fileData = in.readNBytes(filesize);
            fos.write(fileData);
            fos.getFD().sync();
            sendControllerMessage(Protocol.STORE_ACK_TOKEN + " " + filename);
        } catch (IOException e) {
            logger.warning("File store error: " + e.getMessage());
        }
    }

    private void loadData(String message, Socket socket) {
        String path = message.split(" ")[1];
        String filename = path.substring(path.lastIndexOf("\\") + 1);

        try (
                FileInputStream fis = new FileInputStream(new File(file_folder, filename));
                OutputStream out = socket.getOutputStream()
        ) {
            byte[] fileData = fis.readNBytes((int) new File(file_folder, filename).length());
            out.write(fileData);
            out.flush();
            System.out.println("Load of file " + path + " completed");
        } catch (IOException e) {
            logger.warning("File load error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warning("DStore error: " + e.getMessage());
            }
        }
    }

    private void remove(String message) {
        String filename = message.split(" ")[1];
        File target = new File(file_folder, filename);
        if (target.exists() && target.delete()) {
            sendControllerMessage(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        }
    }

    private void list() {
        System.out.println("List received on port " + port);

        StringBuilder sb = new StringBuilder();
        File[] files = file_folder.listFiles();
        if (files != null) {
            for (File f : files) {
                sb.append(" ").append(f.getName());
            }
        }

        sendControllerMessage(Protocol.LIST_TOKEN + sb);
    }

    private void rebalance(String message) {
        String[] tok = message.split(" ");
        int idx = 1;
        int toSend = Integer.parseInt(tok[idx++]);
        byte[] buffer = new byte[8192];

        for (int i = 0; i < toSend; i++) {
            String filename = tok[idx++];
            int numStores = Integer.parseInt(tok[idx++]);
            File f = new File(file_folder, filename);
            int filesize = (int) f.length();

            for (int j = 0; j < numStores; j++) {
                int peerPort = Integer.parseInt(tok[idx++]);
                try (Socket socket = new Socket("localhost", peerPort);
                     FileInputStream fis = new FileInputStream(f)) {

                    socket.setSoTimeout(timeout * 1000);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    OutputStream dataOut = socket.getOutputStream();

                    out.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + filesize);

                    String resp;
                    try {
                        resp = in.readLine();
                    } catch (SocketTimeoutException ste) {
                        logger.warning("Timeout waiting for ACK from port " + peerPort);
                        continue;
                    }
                    if (!Protocol.ACK_TOKEN.equals(resp)) {
                        logger.warning("No ACK from " + peerPort);
                        continue;
                    }

                    int n;
                    while ((n = fis.read(buffer)) > 0) {
                        dataOut.write(buffer, 0, n);
                    }
                    dataOut.flush();
                    socket.shutdownOutput();

                } catch (IOException e) {
                    logger.warning("Rebalance to " + peerPort + " failed: " + e);
                }
            }
        }

        int toRemove = Integer.parseInt(tok[idx++]);
        for (int i = 0; i < toRemove; i++) {
            String toDel = tok[idx++];
            File f = new File(file_folder, toDel);
            if (f.exists()) {
                f.delete();
            }
        }

        sendControllerMessage(Protocol.REBALANCE_COMPLETE_TOKEN);
    }

    private void rebalanceStore(String message, Socket socket) {
        try {
            new PrintWriter(socket.getOutputStream(), true).println(Protocol.ACK_TOKEN);
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }

        String filename = message.split(" ")[1];
        int filesize = Integer.parseInt(message.split(" ")[2]);

        try (InputStream in = socket.getInputStream();
             FileOutputStream fos = new FileOutputStream(new File(file_folder, filename))) {
            byte[] fileData = in.readNBytes(filesize);
            fos.write(fileData);
        } catch (IOException e) {
            logger.warning("File store error: " + e.getMessage());
        }
    }

    private void rebalanceSend(String message, Socket socket) {
        loadData(message, socket);
    }
}