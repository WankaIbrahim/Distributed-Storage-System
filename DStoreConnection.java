import java.io.*;
import java.net.*;

public record DStoreConnection(Socket socket, PrintWriter out, BufferedReader in) {
}