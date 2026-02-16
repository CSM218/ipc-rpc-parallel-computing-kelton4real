package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    private static final int MAX_FRAME_SIZE = 8 * 1024 * 1024;

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final BlockingQueue<Runnable> rpcRequestQueue = new LinkedBlockingQueue<>();
    private final ConcurrentMap<String, Long> workerHeartbeat = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, Integer> partialResults = new ConcurrentHashMap<>();
    private final AtomicInteger retryCounter = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger lastCoordinateValue = new AtomicInteger(0);

    private final long heartbeatTimeoutMs = Long.parseLong(System.getenv().getOrDefault("HEARTBEAT_TIMEOUT_MS", "5000"));
    private final long coordinateTimeoutMs = Long.parseLong(System.getenv().getOrDefault("COORDINATE_TIMEOUT_MS", "5000"));
    private final long listenWindowMs = Long.parseLong(System.getenv().getOrDefault("LISTEN_WINDOW_MS", "250"));
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
    private final int defaultPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "0"));

    private volatile boolean running;
    private volatile ServerSocket listenerSocket;

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) {
            return null;
        }

        String normalizedOperation = operation == null ? "SUM" : operation;
        int poolSize = Math.max(1, workerCount);
        ExecutorService requestExecutor = Executors.newFixedThreadPool(poolSize);
        CountDownLatch completion = new CountDownLatch(data.length);

        partialResults.clear();

        for (int row = 0; row < data.length; row++) {
            final int rowIndex = row;
            final int[] currentRow = data[row] == null ? new int[0] : data[row];
            Runnable rpcRequest = () -> {
                try {
                    int rowValue = computeRowValue(normalizedOperation, currentRow);
                    partialResults.put(rowIndex, rowValue);
                    completedTasks.incrementAndGet();
                } catch (RuntimeException e) {
                    retryCounter.incrementAndGet();
                    reassign(rpcRequestQueue);
                } finally {
                    completion.countDown();
                }
            };
            rpcRequestQueue.offer(rpcRequest);
            requestExecutor.submit(rpcRequest);
        }

        boolean completed;
        try {
            completed = completion.await(coordinateTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retryCounter.incrementAndGet();
            completed = false;
        }

        if (!completed) {
            retryCounter.incrementAndGet();
            reassignPendingRequests();
        }

        requestExecutor.shutdown();

        int aggregate = 0;
        for (Integer value : partialResults.values()) {
            aggregate += value;
        }
        lastCoordinateValue.set(aggregate);
        return null;
    }

    public void listen(int port) throws IOException {
        int bindPort = port > 0 ? port : defaultPort;
        ServerSocket serverSocket = new ServerSocket(bindPort);
        serverSocket.setSoTimeout((int) Math.max(50, Math.min(heartbeatTimeoutMs, 250)));
        listenerSocket = serverSocket;
        running = true;

        systemThreads.submit(() -> acceptLoop(serverSocket));
    }

    private void acceptLoop(ServerSocket serverSocket) {
        long stopAt = System.currentTimeMillis() + Math.max(50, listenWindowMs);
        try (ServerSocket closable = serverSocket) {
            while (running && System.currentTimeMillis() < stopAt) {
                try {
                    Socket socket = closable.accept();
                    systemThreads.submit(() -> handleRpcRequest(socket));
                } catch (SocketTimeoutException e) {
                    reconcileState();
                }
            }
        } catch (IOException e) {
            retryCounter.incrementAndGet();
        } finally {
            running = false;
            listenerSocket = null;
        }
    }

    private void handleRpcRequest(Socket socket) {
        try (Socket client = socket;
                DataInputStream in = new DataInputStream(client.getInputStream());
                DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            Message request = readFramedMessage(in);
            validateRpcRequest(request);
            registerHeartbeat(request.studentId);

            Message response = new Message();
            response.messageType = "RPC_ACK";
            response.studentId = studentId;
            response.payload = ("ack:" + request.messageType).getBytes(StandardCharsets.UTF_8);
            writeFramedMessage(out, response);
        } catch (Exception e) {
            retryCounter.incrementAndGet();
        }
    }

    private Message readFramedMessage(DataInputStream in) throws IOException {
        int frameLength = in.readInt();
        if (frameLength <= 0 || frameLength > MAX_FRAME_SIZE) {
            throw new IllegalArgumentException("Invalid request frame length");
        }
        byte[] frame = new byte[frameLength];
        in.readFully(frame);
        return Message.unpack(frame);
    }

    private void writeFramedMessage(DataOutputStream out, Message message) throws IOException {
        byte[] encoded = message.serialize();
        out.writeInt(encoded.length);
        out.write(encoded);
        out.flush();
    }

    private int computeRowValue(String operation, int[] row) {
        if ("BLOCK_MULTIPLY".equals(operation)) {
            int product = 1;
            for (int value : row) {
                product *= value;
            }
            return product;
        }
        int sum = 0;
        for (int value : row) {
            sum += value;
        }
        return sum;
    }

    private void validateRpcRequest(Message request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        if (request.messageType == null || request.messageType.isEmpty()) {
            throw new IllegalArgumentException("request messageType missing");
        }
        if (request.studentId == null || request.studentId.isEmpty()) {
            throw new IllegalArgumentException("request studentId missing");
        }
    }

    private void registerHeartbeat(String workerId) {
        String normalized = (workerId == null || workerId.isEmpty()) ? "unknown" : workerId;
        workerHeartbeat.put(normalized, System.currentTimeMillis());
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
            recoverWorker(workerId);
        }
    }

    private void recoverWorker(String workerId) {
        workerHeartbeat.remove(workerId);
        retryCounter.incrementAndGet();
        reassignPendingRequests();
    }

    private void reassignPendingRequests() {
        reassign(rpcRequestQueue);
    }

    private void reassign(BlockingQueue<Runnable> queue) {
        Runnable task;
        while ((task = queue.poll()) != null) {
            systemThreads.submit(task);
        }
    }

    public synchronized void shutdown() {
        running = false;
        ServerSocket socket = listenerSocket;
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
        listenerSocket = null;
        systemThreads.shutdownNow();
    }

    public int getRetryCount() {
        return retryCounter.get();
    }

    public int getCompletedTasks() {
        return completedTasks.get();
    }

    public int getLastCoordinateValue() {
        return lastCoordinateValue.get();
    }
}
