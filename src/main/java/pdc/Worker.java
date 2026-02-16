package pdc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
    private final int joinRetries = Integer.parseInt(System.getenv().getOrDefault("JOIN_RETRIES", "3"));

    public Worker() {
    }

    public Worker(String workerId, String masterHost, int masterPort) {
        System.setProperty("pdc.worker.id", workerId == null ? "worker-unknown" : workerId);
        System.setProperty("pdc.master.host", masterHost == null ? "localhost" : masterHost);
        System.setProperty("pdc.master.port", String.valueOf(masterPort));
    }

    public void connect() {
        joinCluster(configuredMasterHost, configuredMasterPort);
    }

    public void run() {
        connect();
        execute();
    }

    public void joinCluster(String masterHost, int port) {
        String host = (masterHost == null || masterHost.isEmpty()) ? resolveMasterHost() : masterHost;
        int targetPort = resolveMasterPort(port);

        int attempts = Math.max(1, joinRetries);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, targetPort), timeoutMs);
                socket.setSoTimeout(timeoutMs);

                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

                send(writer, buildMessage(Message.CONNECT, workerId));
                readAndStore(reader);

                send(writer, buildMessage(Message.REGISTER_WORKER, workerId));
                readAndStore(reader);

                send(writer, buildMessage(Message.REGISTER_CAPABILITIES, "matrix-multiply,heartbeat"));
                readAndStore(reader);

                send(writer, buildMessage(Message.HEARTBEAT, "alive"));
                readAndStore(reader);

                health.put("master", System.currentTimeMillis());
                return;
            } catch (Exception e) {
                health.put("joinFailure", System.currentTimeMillis());
            }
        }
    }

    public void processTask(String taskId, String payload) {
        Message message = buildMessage(Message.RPC_REQUEST, taskId + ";MATRIX_MULTIPLY;" + payload);
        requestQueue.offer(message);
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

    private String resolveMasterHost() {
        String propertyHost = System.getProperty("pdc.master.host", "");
        if (!propertyHost.isEmpty()) {
            return propertyHost;
        }
        return configuredMasterHost;
    }

    private int resolveMasterPort(int portArg) {
        if (portArg > 0) {
            return portArg;
        }
        int propertyPort = Integer.parseInt(System.getProperty("pdc.master.port", "0"));
        if (propertyPort > 0) {
            return propertyPort;
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

    private void send(BufferedWriter writer, Message message) throws Exception {
        writer.write(message.toJson());
        writer.newLine();
        writer.flush();
    }

    private void readAndStore(BufferedReader reader) throws Exception {
        String line = reader.readLine();
        if (line == null || line.isEmpty()) {
            throw new IllegalArgumentException("Missing response");
        }
        Message response = Message.parse(line);
        parseResponse(response);
    }

    private void parseResponse(Message response) {
        if (response == null) {
            throw new IllegalArgumentException("Invalid response");
        }
        if (response.messageType == null || response.messageType.isEmpty()) {
            throw new IllegalArgumentException("Invalid response type");
        }
        if (!Message.WORKER_ACK.equals(response.messageType)
                && !Message.RPC_RESPONSE.equals(response.messageType)
                && !Message.TASK_COMPLETE.equals(response.messageType)
                && !Message.TASK_ERROR.equals(response.messageType)) {
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

    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.run();
        try {
            while (true) {
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
