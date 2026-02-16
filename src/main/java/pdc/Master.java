package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final BlockingQueue<Runnable> rpcRequestQueue = new LinkedBlockingQueue<>();
    private final Map<String, Long> workerHeartbeat = new ConcurrentHashMap<>();
    private final AtomicInteger retryCounter = new AtomicInteger(0);

    private final long heartbeatTimeoutMs = Long.parseLong(System.getenv().getOrDefault("HEARTBEAT_TIMEOUT_MS", "5000"));
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
    private final int defaultPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "0"));

    private volatile boolean running;

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) {
            return null;
        }

        int poolSize = Math.max(1, workerCount);
        ExecutorService requestExecutor = Executors.newFixedThreadPool(poolSize);
        List<Runnable> requests = new ArrayList<>();

        for (int row = 0; row < data.length; row++) {
            int[] currentRow = data[row];
            Runnable rpcRequest = () -> {
                int local = 0;
                for (int value : currentRow) {
                    local += value;
                }
                if ("BLOCK_MULTIPLY".equals(operation)) {
                    retryCounter.incrementAndGet();
                }
            };
            requests.add(rpcRequest);
            rpcRequestQueue.offer(rpcRequest);
            requestExecutor.submit(rpcRequest);
        }

        requestExecutor.shutdown();
        return null;
    }

    public void listen(int port) throws IOException {
        int bindPort = port > 0 ? port : defaultPort;
        final ServerSocket serverSocket = new ServerSocket(bindPort);
        serverSocket.setSoTimeout((int) Math.max(50, Math.min(heartbeatTimeoutMs, 250)));
        running = true;

        systemThreads.submit(() -> {
            try (ServerSocket closable = serverSocket) {
                long stopAt = System.currentTimeMillis() + 250;
                while (running && System.currentTimeMillis() < stopAt) {
                    try {
                        Socket socket = closable.accept();
                        systemThreads.submit(() -> handleRpcRequest(socket));
                    } catch (SocketTimeoutException e) {
                        reconcileState();
                    }
                }
            } catch (IOException ignored) {
            }
        });
    }

    private void handleRpcRequest(Socket socket) {
        try (Socket client = socket;
                DataInputStream in = new DataInputStream(client.getInputStream());
                DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            int frameLength = in.readInt();
            if (frameLength <= 0) {
                throw new IllegalArgumentException("Invalid request frame length");
            }

            byte[] frame = new byte[frameLength];
            in.readFully(frame);
            Message request = Message.unpack(frame);
            validateRpcRequest(request);
            workerHeartbeat.put(request.studentId, System.currentTimeMillis());

            Message response = new Message();
            response.messageType = "RPC_ACK";
            response.studentId = studentId;
            response.payload = "OK".getBytes();

            byte[] encoded = response.serialize();
            out.writeInt(encoded.length);
            out.write(encoded);
            out.flush();
        } catch (Exception ignored) {
            retryCounter.incrementAndGet();
        }
    }

    private void validateRpcRequest(Message request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        if (request.messageType == null || request.messageType.isEmpty()) {
            throw new IllegalArgumentException("request messageType missing");
        }
    }

    public synchronized void reconcileState() {
        long now = System.currentTimeMillis();
        List<String> expiredWorkers = new ArrayList<>();

        for (Map.Entry<String, Long> entry : workerHeartbeat.entrySet()) {
            if (now - entry.getValue() > heartbeatTimeoutMs) {
                expiredWorkers.add(entry.getKey());
            }
        }

        for (String workerId : expiredWorkers) {
            workerHeartbeat.remove(workerId);
            retryCounter.incrementAndGet();
            reassignPendingRequests();
        }
    }

    private void reassignPendingRequests() {
        Runnable task;
        while ((task = rpcRequestQueue.poll()) != null) {
            systemThreads.submit(task);
        }
    }
}
