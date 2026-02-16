package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker {

    private final ExecutorService workerPool = Executors.newFixedThreadPool(2);
    private final BlockingQueue<Message> requestQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Long> health = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final String studentId = "anonymous";
    private final int timeoutMs = 500;

    public void joinCluster(String masterHost, int port) {
        String host = masterHost == null || masterHost.isEmpty() ? "localhost" : masterHost;
        int targetPort = port > 0 ? port : 9999;

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, targetPort), timeoutMs);
            socket.setSoTimeout(timeoutMs);

            Message registrationRequest = new Message();
            registrationRequest.messageType = "RPC_REGISTER";
            registrationRequest.studentId = studentId;
            registrationRequest.payload = "heartbeat".getBytes();

            byte[] frame = registrationRequest.serialize();

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(frame.length);
            out.write(frame);
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            int responseLen = in.readInt();
            if (responseLen > 0) {
                byte[] response = new byte[responseLen];
                in.readFully(response);
                Message ack = Message.unpack(response);
                parseResponse(ack);
            }
        } catch (Exception ignored) {
        }
    }

    private void parseResponse(Message response) {
        if (response == null || response.messageType == null) {
            throw new IllegalArgumentException("Invalid response");
        }
        health.put("master", System.currentTimeMillis());
        requestQueue.offer(response);
    }

    public void execute() {
        workerPool.submit(() -> {
            while (running.get()) {
                try {
                    Message request = requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        handleRequest(request);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running.set(false);
                }
            }
        });
    }

    private void handleRequest(Message request) {
        if (request.messageType != null && request.messageType.contains("RPC")) {
            health.put("lastRequest", System.currentTimeMillis());
        }
    }
}
