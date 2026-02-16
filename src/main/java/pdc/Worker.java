package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker {

    private static final int MAX_FRAME_SIZE = 8 * 1024 * 1024;

    private final ExecutorService workerPool = Executors.newFixedThreadPool(2);
    private final BlockingQueue<Message> requestQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Long> health = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
    private final int timeoutMs = Integer.parseInt(System.getenv().getOrDefault("WORKER_TIMEOUT_MS", "500"));
    private final int joinRetries = Integer.parseInt(System.getenv().getOrDefault("JOIN_RETRIES", "2"));

    public void joinCluster(String masterHost, int port) {
        String host = masterHost == null || masterHost.isEmpty() ? "localhost" : masterHost;
        int targetPort = port > 0 ? port : Integer.parseInt(System.getenv().getOrDefault("PORT", "9999"));

        int attempts = Math.max(1, joinRetries);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, targetPort), timeoutMs);
                socket.setSoTimeout(timeoutMs);

                Message registrationRequest = new Message();
                registrationRequest.messageType = "RPC_REGISTER";
                registrationRequest.studentId = studentId;
                registrationRequest.payload = "heartbeat".getBytes(StandardCharsets.UTF_8);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                writeFramedMessage(out, registrationRequest);

                DataInputStream in = new DataInputStream(socket.getInputStream());
                Message ack = readFramedMessage(in);
                parseResponse(ack);
                sendHeartbeat(out);
                return;
            } catch (Exception e) {
                health.put("joinFailure", System.currentTimeMillis());
            }
        }
    }

    private void sendHeartbeat(DataOutputStream out) throws IOException {
        Message heartbeat = new Message();
        heartbeat.messageType = "RPC_HEARTBEAT";
        heartbeat.studentId = studentId;
        heartbeat.payload = "ping".getBytes(StandardCharsets.UTF_8);
        writeFramedMessage(out, heartbeat);
    }

    private void writeFramedMessage(DataOutputStream out, Message message) throws IOException {
        byte[] frame = message.serialize();
        out.writeInt(frame.length);
        out.write(frame);
        out.flush();
    }

    private Message readFramedMessage(DataInputStream in) throws IOException {
        int frameLength = in.readInt();
        if (frameLength <= 0 || frameLength > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Invalid response frame length");
        }
        byte[] response = new byte[frameLength];
        in.readFully(response);
        return Message.unpack(response);
    }

    private void parseResponse(Message response) {
        if (response == null || response.messageType == null || response.messageType.isEmpty()) {
            throw new IllegalArgumentException("Invalid response");
        }
        if (!"RPC_ACK".equals(response.messageType)) {
            throw new IllegalArgumentException("Unexpected response type");
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

    public void enqueue(Message request) {
        if (request != null) {
            requestQueue.offer(request);
        }
    }

    public void shutdown() {
        running.set(false);
        workerPool.shutdownNow();
    }
}
