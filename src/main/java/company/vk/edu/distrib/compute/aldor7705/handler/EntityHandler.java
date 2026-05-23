package company.vk.edu.distrib.compute.aldor7705.handler;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.aldor7705.exceptions.MethodNotAllowedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class EntityHandler extends BaseHttpHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private final List<Integer> clusterPorts;
    private final int myPort;
    private final Dao<byte[]> dao;
    private final Consumer<AuditEvent> auditSink;

    public EntityHandler(Dao<byte[]> dao, int myPort, List<Integer> clusterPorts, Consumer<AuditEvent> auditSink) {
        super();
        this.dao = dao;
        this.myPort = myPort;
        this.clusterPorts = clusterPorts;
        this.auditSink = auditSink;
    }

    @Override
    protected void handleRequest(HttpExchange exchange) throws IOException {
        long requestTimestamp = System.currentTimeMillis();
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        String id = getIdFromQuery(query);
        auditSink.accept(new AuditEvent(method, id, requestTimestamp));

        byte[] requestBody = null;
        if ("PUT".equals(method)) {
            requestBody = exchange.getRequestBody().readAllBytes();
        }

        if (clusterPorts != null && !clusterPorts.isEmpty()) {
            int targetPort = getTargetPort(id);

            if (myPort != targetPort) {
                proxyToNode(exchange, targetPort, id, method, requestBody);
                return;
            }
        }

        switch (method) {
            case "GET":
                byte[] data = getEntityDao(id);
                sendAnswer(exchange, data, 200);
                break;
            case "PUT":
                dao.upsert(id, requestBody);
                sendEmptyAnswer(exchange, 201);
                break;
            case "DELETE":
                dao.delete(id);
                sendEmptyAnswer(exchange, 202);
                break;
            default:
                throw new MethodNotAllowedException("Метод " + method + " не поддерживается для /entity");
        }
    }

    private int getTargetPort(String key) {
        int index = Math.abs(key.hashCode()) % clusterPorts.size();
        return clusterPorts.get(index);
    }

    private void proxyToNode(HttpExchange exchange, int targetPort,
                             String id, String method, byte[] requestBody) throws IOException {
        String targetUrl = "http://localhost:" + targetPort + "/v0/entity?id=" + id;

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .method(method,
                        requestBody != null
                                ? HttpRequest.BodyPublishers.ofByteArray(requestBody)
                                : HttpRequest.BodyPublishers.noBody());

        try {
            HttpResponse<byte[]> response = httpClient.send(
                    requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            exchange.getResponseHeaders().putAll(response.headers().map());
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);

            try (var outputStream = exchange.getResponseBody()) {
                outputStream.write(response.body());
            }
        } catch (Exception e) {
            log.error("Ошибка кластера", e);
            sendError(exchange, 500, "Ошибка кластера: " + e.getMessage());
        }
    }

    private String getIdFromQuery(String query) {
        if (query == null || !query.startsWith("id=")) {
            log.warn("Ошибка при попытке получить id");
            throw new IllegalArgumentException("id отсутствует");
        }
        String id = query.substring(3);
        if (id.isEmpty()) {
            log.warn("Ошибка при попытке получить id");
            throw new IllegalArgumentException("id пуст");
        }
        return id;
    }

    private byte[] getEntityDao(String id) throws IOException {
        try {
            return dao.get(id);
        } catch (NoSuchElementException e) {
            log.warn("Ключ {} не найден", id);
            throw new NoSuchElementException("Ключ не найден: " + id, e);
        }
    }
}
