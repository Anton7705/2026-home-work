package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class AuditServiceSimple implements AuditService {
    private static final Logger log = LoggerFactory.getLogger(AuditServiceSimple.class);
    private static final String AUDIT_TOPIC = "audit";

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final Path auditFile;
    private final List<AuditEvent> entries = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<KafkaConsumer<String, String>> consumerRef = new AtomicReference<>();

    private Thread consumerThread;

    public AuditServiceSimple(String bootstrapServers, String consumerGroupId) throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.auditFile = Files.createTempFile("audit-" + sanitize(consumerGroupId) + "-", ".log");
    }

    @Override
    public synchronized void start() {
        if (running.get()) {
            return;
        }

        running.set(true);
        consumerThread = new Thread(this::runConsumer, "audit-consumer-" + consumerGroupId);
        consumerThread.start();
    }

    @Override
    public synchronized void stop() {
        if (!running.get()) {
            return;
        }

        running.set(false);
        KafkaConsumer<String, String> consumer = consumerRef.get();
        if (consumer != null) {
            consumer.wakeup();
        }
        try {
            if (consumerThread != null) {
                consumerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumerThread = null;
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        lock.lock();
        try {
            return List.copyOf(entries);
        } finally {
            lock.unlock();
        }
    }

    private void runConsumer() {
        Properties props = consumerProperties(consumerGroupId);

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
            consumerRef.set(kafkaConsumer);
            kafkaConsumer.subscribe(List.of(AUDIT_TOPIC));

            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        continue;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        AuditEvent event = AuditEventSerdeUtils.deserialize(record.value());
                        save(event);
                    }
                    kafkaConsumer.commitSync();
                } catch (WakeupException e) {
                    if (running.get()) {
                        log.error("Неожиданный WakeupException", e);
                    }
                    break;
                } catch (Exception e) {
                    log.error("Ошибка чтения событий аудита", e);
                }
            }
        } catch (Exception e) {
            log.error("Ошибка при работе consumer", e);
        } finally {
            consumerRef.set(null);
        }
    }

    private void save(AuditEvent event) {
        lock.lock();
        try {
            entries.add(event);
        } finally {
            lock.unlock();
        }

        try {
            Files.writeString(
                    auditFile,
                    AuditEventSerdeUtils.serialize(event) + System.lineSeparator(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка сохранения события аудита", e);
        }
    }

    private Properties consumerProperties(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
