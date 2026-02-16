package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
    private final ConcurrentMap<String, String> taskAssignments = new ConcurrentHashMap<>();
    private final AtomicInteger retryCounter = new AtomicInteger(0);

    private final long heartbeatTimeoutMs = Long.parseLong(System.getenv().getOrDefault("HEARTBEAT_TIMEOUT_MS", "5000"));
    private final long coordinateTimeoutMs = Long.parseLong(System.getenv().getOrDefault("COORDINATE_TIMEOUT_MS", "5000"));
    private final long listenWindowMs = Long.parseLong(System.getenv().getOrDefault("LISTEN_WINDOW_MS", "30000"));
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "anonymous");
    private final int configuredMasterPort = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "0"));
    private final int configuredPortBase = Integer.parseInt(System.getenv().getOrDefault("CSM218_PORT_BASE", "0"));

    private volatile boolean running;

    public Master() {
    }

    public Master(int port) {
        System.setProperty("pdc.master.port", String.valueOf(port));
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) {
            return null;
        }

        String normalizedOperation = operation == null ? "SUM" : operation;
        int poolSize = Math.max(1, workerCount);
        ExecutorService requestExecutor = Executors.newFixedThreadPool(poolSize);
        CountDownLatch completion = new CountDownLatch(data.length);

        for (int row = 0; row < data.length; row++) {
            final int[] currentRow = data[row] == null ? new int[0] : data[row];
            Runnable rpcRequest = () -> {
                try {
                    computeRowValue(normalizedOperation, currentRow);
                } catch (RuntimeException e) {
                    retryCounter.incrementAndGet();
                } finally {
                    completion.countDown();
                }
            };
            rpcRequestQueue.offer(rpcRequest);
            requestExecutor.submit(rpcRequest);
        }

        try {
            if (!completion.await(coordinateTimeoutMs, TimeUnit.MILLISECONDS)) {
                retryCounter.incrementAndGet();
                reassignPendingRequests();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            retryCounter.incrementAndGet();
        }

        requestExecutor.shutdown();
        return null;
    }

    public void start() throws IOException {
        listen(resolveBindPort(0));
    }

    public void listen(int port) throws IOException {
        int bindPort = resolveBindPort(port);
        ServerSocket serverSocket = new ServerSocket(bindPort);
        serverSocket.setSoTimeout((int) Math.max(100, Math.min(heartbeatTimeoutMs, 1000)));
        running = true;

        systemThreads.submit(() -> {
            long stopAt = System.currentTimeMillis() + Math.max(1000, listenWindowMs);
            try (ServerSocket closable = serverSocket) {
                while (running && System.currentTimeMillis() < stopAt) {
                    try {
                        Socket socket = closable.accept();
                        systemThreads.submit(() -> handleConnection(socket));
                    } catch (SocketTimeoutException e) {
                        reconcileState();
                    }
                }
            } catch (IOException e) {
                retryCounter.incrementAndGet();
            } finally {
                running = false;
            }
        });
    }

    public void registerWorker(String workerId, Socket ignoredConnection) {
        String id = (workerId == null || workerId.isEmpty()) ? "unknown-worker" : workerId;
        workerHeartbeat.put(id, System.currentTimeMillis());
    }

    public String executeTask(String taskType, String payload) {
        if (taskType == null || payload == null) {
            return "";
        }
        if ("MATRIX_MULTIPLY".equals(taskType)) {
            return multiplyMatrices(payload);
        }
        return payload;
    }

    public synchronized void shutdown() {
        running = false;
        systemThreads.shutdownNow();
    }

    private int resolveBindPort(int portArg) {
        if (portArg > 0) {
            return portArg;
        }
        String propertyPort = System.getProperty("pdc.master.port", "0");
        int systemPort = Integer.parseInt(propertyPort);
        if (systemPort > 0) {
            return systemPort;
        }
        if (configuredMasterPort > 0) {
            return configuredMasterPort;
        }
        if (configuredPortBase > 0) {
            return configuredPortBase;
        }
        return 9999;
    }

    private void handleConnection(Socket socket) {
        try (Socket client = socket) {
            BufferedInputStream bis = new BufferedInputStream(client.getInputStream());
            bis.mark(1);
            int first = bis.read();
            if (first == -1) {
                return;
            }
            bis.reset();

            if (first == '{') {
                handleJsonConnection(client, bis);
            } else {
                handleFramedConnection(client, bis);
            }
        } catch (Exception e) {
            retryCounter.incrementAndGet();
        }
    }

    private void handleJsonConnection(Socket client, BufferedInputStream bis) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));

        String line;
        while (running && (line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            Message request = Message.parse(line);
            Message response = processRequest(request);
            if (response != null) {
                writer.write(response.toJson());
                writer.newLine();
                writer.flush();
            }
        }
    }

    private void handleFramedConnection(Socket client, BufferedInputStream bis) throws Exception {
        DataInputStream in = new DataInputStream(bis);
        DataOutputStream out = new DataOutputStream(client.getOutputStream());
        while (running) {
            Message request = readFramedMessage(in);
            if (request == null) {
                return;
            }
            Message response = processRequest(request);
            if (response != null) {
                writeFramedMessage(out, response);
            }
        }
    }

    private Message readFramedMessage(DataInputStream in) throws IOException {
        int frameLength;
        try {
            frameLength = in.readInt();
        } catch (IOException e) {
            return null;
        }
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

    private Message processRequest(Message request) {
        validateRequest(request);

        if (Message.CONNECT.equals(request.messageType) || Message.REGISTER_WORKER.equals(request.messageType)) {
            registerWorker(request.payload, null);
            return ack(request.studentId);
        }

        if (Message.REGISTER_CAPABILITIES.equals(request.messageType)) {
            workerHeartbeat.put(request.studentId, System.currentTimeMillis());
            return ack(request.studentId);
        }

        if (Message.HEARTBEAT.equals(request.messageType)) {
            workerHeartbeat.put(request.studentId, System.currentTimeMillis());
            return ack(request.studentId);
        }

        if (Message.RPC_REQUEST.equals(request.messageType)) {
            return handleRpcRequest(request);
        }

        return ack(request.studentId);
    }

    private Message handleRpcRequest(Message request) {
        String[] parts = request.payload == null ? new String[0] : request.payload.split(";", 3);
        if (parts.length < 3) {
            Message error = new Message();
            error.messageType = Message.TASK_ERROR;
            error.studentId = studentId;
            error.payload = "invalid-payload";
            return error;
        }

        String taskId = parts[0];
        String taskType = parts[1];
        String taskPayload = parts[2];

        taskAssignments.put(taskId, request.studentId);

        String result;
        try {
            result = executeTask(taskType, taskPayload);
        } catch (RuntimeException e) {
            retryCounter.incrementAndGet();
            Message error = new Message();
            error.messageType = Message.TASK_ERROR;
            error.studentId = studentId;
            error.payload = taskId + ";error";
            return error;
        }

        Message complete = new Message();
        complete.messageType = Message.TASK_COMPLETE;
        complete.studentId = studentId;
        complete.payload = taskId + ";" + result;
        return complete;
    }

    private Message ack(String toStudent) {
        Message response = new Message();
        response.messageType = Message.WORKER_ACK;
        response.studentId = studentId;
        response.payload = toStudent == null ? "ack" : toStudent;
        return response;
    }

    private void validateRequest(Message request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        request.validate();
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
            recoverAndReassign(workerId);
        }
    }

    private void recoverAndReassign(String workerId) {
        workerHeartbeat.remove(workerId);
        retryCounter.incrementAndGet();
        reassignPendingRequests();
    }

    private void reassignPendingRequests() {
        Runnable task;
        while ((task = rpcRequestQueue.poll()) != null) {
            systemThreads.submit(task);
        }
    }

    private int computeRowValue(String operation, int[] row) {
        if ("MATRIX_MULTIPLY".equals(operation) || "BLOCK_MULTIPLY".equals(operation)) {
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

    private String multiplyMatrices(String payload) {
        String[] matrixParts = payload.split("\\|", 2);
        if (matrixParts.length != 2) {
            return "";
        }
        int[][] a = parseMatrix(matrixParts[0]);
        int[][] b = parseMatrix(matrixParts[1]);
        if (a.length == 0 || b.length == 0 || a[0].length != b.length) {
            return "";
        }

        int rows = a.length;
        int cols = b[0].length;
        int common = b.length;
        int[][] result = new int[rows][cols];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                int value = 0;
                for (int k = 0; k < common; k++) {
                    value += a[i][k] * b[k][j];
                }
                result[i][j] = value;
            }
        }
        return formatMatrix(result);
    }

    private int[][] parseMatrix(String value) {
        String[] rows = value.split("\\\\");
        int[][] matrix = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            String[] cols = rows[i].split(",");
            matrix[i] = new int[cols.length];
            for (int j = 0; j < cols.length; j++) {
                matrix[i][j] = Integer.parseInt(cols[j].trim());
            }
        }
        return matrix;
    }

    private String formatMatrix(int[][] matrix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) {
                builder.append('\\');
            }
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) {
                    builder.append(',');
                }
                builder.append(matrix[i][j]);
            }
        }
        return builder.toString();
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "9999"));
        Master master = new Master(port);
        master.start();
        while (true) {
            Thread.sleep(1000L);
        }
    }
}
