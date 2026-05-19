package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AuditServiceSimple implements AuditService {
    private static final Logger log = LoggerFactory.getLogger(AuditServiceSimple.class);
    private static final String AUDIT_TOPIC = "audit";
    private static final long POLL_TIMEOUT_MS = 1000;

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final Path storagePath;

    private KafkaConsumer<String, String> consumer;
    private ExecutorService executor;
    private AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean stopped = false;

    private final List<AuditEvent> auditEvents = Collections.synchronizedList(new ArrayList<>());
    private final Set<String> processedOffsets = Collections.synchronizedSet(new HashSet<>());

    public AuditServiceSimple(String bootstrapServers, String consumerGroupId) throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.storagePath = Path.of("audit_storage_" + consumerGroupId);

        if (!Files.exists(storagePath)) {
            Files.createDirectories(storagePath);
        }

        loadPersistedEvents();
    }

    private void loadPersistedEvents() {
        try {
            Path eventsFile = storagePath.resolve("events.txt");
            if (Files.exists(eventsFile)) {
                try (BufferedReader reader = Files.newBufferedReader(eventsFile)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        AuditEvent event = parseEventFromLine(line);
                        if (event != null) {
                            auditEvents.add(event);
                        }
                    }
                }
            }

            Path offsetsFile = storagePath.resolve("offsets.txt");
            if (Files.exists(offsetsFile)) {
                try (BufferedReader reader = Files.newBufferedReader(offsetsFile)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        processedOffsets.add(line);
                    }
                }
            }
        } catch (IOException e) {
            log.error("Failed to load persisted events", e);
        }
    }

    private void persistEvent(AuditEvent event) {
        try {
            Path eventsFile = storagePath.resolve("events.txt");
            String line = String.format("%s|%s|%d%n", event.method(), event.id(), event.timestamp());
            Files.write(eventsFile, line.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to persist event", e);
        }
    }

    private void persistOffset(String topicPartitionOffset) {
        try {
            Path offsetsFile = storagePath.resolve("offsets.txt");
            String line = topicPartitionOffset + "\n";
            Files.write(offsetsFile, line.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to persist offset", e);
        }
    }

    private AuditEvent parseEventFromLine(String line) {
        try {
            String[] parts = line.split("\\|");
            if (parts.length == 3) {
                return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
            }
        } catch (Exception e) {
            log.error("Failed to parse event line: {}", line, e);
        }
        return null;
    }

    private void initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(AUDIT_TOPIC));
    }

    @Override
    public void start() {
        if (running.get()) {
            return;
        }

        running.set(true);
        stopped = false;
        initConsumer();

        executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (running.get() && !stopped) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                    for (ConsumerRecord<String, String> record : records) {
                        String offsetKey = String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());

                        if (!processedOffsets.contains(offsetKey)) {
                            AuditEvent event = parseAuditEvent(record.value());
                            if (event != null) {
                                auditEvents.add(event);
                                persistEvent(event);
                                processedOffsets.add(offsetKey);
                                persistOffset(offsetKey);
                            }
                        }
                    }

                    if (!records.isEmpty()) {
                        consumer.commitSync();
                    }
                } catch (Exception e) {
                    log.error("Error consuming messages", e);
                }
            }

            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
        });
    }

    private AuditEvent parseAuditEvent(String json) {
        try {
            String method = extractJsonValue(json, "method");
            String id = extractJsonValue(json, "id");
            String timestampStr = extractJsonValue(json, "timestamp");

            if (method != null && id != null && timestampStr != null) {
                return new AuditEvent(method, id, Long.parseLong(timestampStr));
            }
        } catch (Exception e) {
            log.error("Failed to parse audit event: {}", json, e);
        }
        return null;
    }

    private String extractJsonValue(String json, String key) {
        String searchKey = "\"" + key + "\":";
        int keyIndex = json.indexOf(searchKey);
        if (keyIndex == -1) return null;

        int startQuote = json.indexOf("\"", keyIndex + searchKey.length());
        if (startQuote == -1) {
            int start = keyIndex + searchKey.length();
            int end = json.indexOf(",", start);
            if (end == -1) end = json.indexOf("}", start);
            if (end != -1) {
                return json.substring(start, end).trim();
            }
            return null;
        }

        int endQuote = json.indexOf("\"", startQuote + 1);
        if (endQuote != -1) {
            return json.substring(startQuote + 1, endQuote);
        }
        return null;
    }

    @Override
    public void stop() {
        if (!running.get()) {
            return;
        }

        stopped = true;
        running.set(false);

        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (consumer != null) {
            try {
                consumer.wakeup();
            } catch (Exception e) {

            }
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return new ArrayList<>(auditEvents);
    }
}
