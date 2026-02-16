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

    private static final int MAX_FRAME_SIZE = 8 * 1024 * 1024;

    private final ExecutorService workerPool = Executors.newFixedThreadPool(2);
    private final BlockingQueue<Message> requestQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Long> health = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
    private final String workerId = System.getenv().getOrDefault("WORKER_ID", "worker-unknown");
    private final String configuredMasterHost = System.getenv().getOrDefault("MASTER_HOST", "localhost");
    private final int configuredMasterPort = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "9999"));
    private final int configuredPortBase = Integer.parseInt(System.getenv().getOrDefault("CSM218_PORT_BASE", "0"));
    private final int timeoutMs = Integer.parseInt(System.getenv().getOrDefault("WORKER_TIMEOUT_MS", "500"));
    private final int joinRetries = Integer.parseInt(System.getenv().getOrDefault("JOIN_RETRIES", "2"));

    public void joinCluster(String masterHost, int port) {
        String host = (masterHost == null || masterHost.isEmpty()) ? configuredMasterHost : masterHost;
        int targetPort = resolveMasterPort(port);

        int attempts = Math.max(1, joinRetries);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, targetPort), timeoutMs);
                socket.setSoTimeout(timeoutMs);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());

                writeFramedMessage(out, buildMessage(Message.CONNECT, workerId));
                readAck(in);

                writeFramedMessage(out, buildMessage(Message.REGISTER_WORKER, workerId));
                readAck(in);

                writeFramedMessage(out, buildMessage(Message.REGISTER_CAPABILITIES, "matrix-multiply,heartbeat"));
                readAck(in);

                writeFramedMessage(out, buildMessage(Message.HEARTBEAT, "alive"));
                health.put("master", System.currentTimeMillis());
                return;
            } catch (Exception e) {
                health.put("joinFailure", System.currentTimeMillis());
            }
        }
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

    private int resolveMasterPort(int portArg) {
        if (portArg > 0) {
            return portArg;
        }
        if (configuredMasterPort > 0) {
            return configuredMasterPort;
        }
        if (configuredPortBase > 0) {
            return configuredPortBase;
        }
        return 9999;
    }

    private Message buildMessage(String messageType, String payload) {
        Message message = new Message();
        message.messageType = messageType;
        message.studentId = studentId;
        message.payload = payload == null ? "" : payload;
        return message;
    }

    private void readAck(DataInputStream in) throws Exception {
        Message ack = readFramedMessage(in);
        parseResponse(ack);
    }

    private void writeFramedMessage(DataOutputStream out, Message message) throws Exception {
        byte[] frame = message.serialize();
        out.writeInt(frame.length);
        out.write(frame);
        out.flush();
    }

    private Message readFramedMessage(DataInputStream in) throws Exception {
        int frameLength = in.readInt();
        if (frameLength <= 0 || frameLength > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Invalid response frame length");
        }
        byte[] frame = new byte[frameLength];
        in.readFully(frame);
        return Message.unpack(frame);
    }

    private void parseResponse(Message response) {
        if (response == null) {
            throw new IllegalArgumentException("Invalid response");
        }
        if (response.messageType == null || response.messageType.isEmpty()) {
            throw new IllegalArgumentException("Invalid response type");
        }
        if (!Message.WORKER_ACK.equals(response.messageType) && !Message.RPC_RESPONSE.equals(response.messageType)) {
            throw new IllegalArgumentException("Unexpected response type");
        }
        requestQueue.offer(response);
        health.put("lastAck", System.currentTimeMillis());
    }

    private void handleRequest(Message request) {
        if (request.messageType != null && request.messageType.contains("RPC")) {
            health.put("lastRequest", System.currentTimeMillis());
        }
    }
}
