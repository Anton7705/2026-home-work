package company.vk.edu.distrib.compute.aldor7705;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.aldor7705.handler.EntityHandler;
import company.vk.edu.distrib.compute.aldor7705.handler.StatusHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

public class KVServiceSimple implements AuditableKVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceSimple.class);
    private static final String AUDIT_TOPIC = "audit";
    private final int port;
    private final Dao<byte[]> dao;
    private final List<Integer> clusterPorts;
    private HttpServer httpServer;
    private KafkaProducer<String, String> auditProducer;
    private String bootstrapServers;
    private boolean async = true;
    private boolean running;

    public KVServiceSimple(int port, Dao<byte[]> dao, List<Integer> clusterPorts) {
        this.port = port;
        this.dao = dao;
        this.clusterPorts = clusterPorts;
    }

    private HttpServer createServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao, port, clusterPorts, this::sendAuditEvent));
        return server;
    }

    @Override
    public void start() {
        if (!running) {
            try {
                httpServer = createServer();
                createProducerIfConfigured();
                log.info("Запуск сервиса на порту {}", port);
                httpServer.start();
                running = true;
            } catch (IOException e) {
                log.error("Ошибка при запуске", e);
            }
        }
    }

    @Override
    public void stop() {
        if (running) {
            log.info("Остановка сервиса на порту {}", port);
            httpServer.stop(0);
            closeProducer();
            running = false;
        }
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        if (running) {
            closeProducer();
            createProducerIfConfigured();
        }
    }

    @Override
    public void setAsync(boolean enabled) {
        this.async = enabled;
    }

    private void sendAuditEvent(AuditEvent auditEvent) {
        KafkaProducer<String, String> producer = auditProducer;
        if (producer == null) {
            return;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(
                AUDIT_TOPIC,
                auditEvent.id(),
                AuditEventSerdeUtils.serialize(auditEvent)
        );

        try {
            if (async) {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Ошибка асинхронной отправки события аудита", exception);
                    }
                });
            } else {
                producer.send(record).get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Отправка события аудита прервана", e);
        } catch (Exception e) {
            log.error("Ошибка отправки события аудита", e);
        }
    }

    private void createProducerIfConfigured() {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return;
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        auditProducer = new KafkaProducer<>(properties);
    }

    private void closeProducer() {
        if (auditProducer != null) {
            auditProducer.flush();
            auditProducer.close();
            auditProducer = null;
        }
    }
}
