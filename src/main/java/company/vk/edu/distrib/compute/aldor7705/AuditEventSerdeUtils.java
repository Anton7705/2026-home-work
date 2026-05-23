package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.AuditEvent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class AuditEventSerdeUtils {
    private static final String DELIMITER = "\t";

    private AuditEventSerdeUtils() {
    }

    static String serialize(AuditEvent event) {
        return event.method()
                + DELIMITER
                + Base64.getEncoder().encodeToString(event.id().getBytes(StandardCharsets.UTF_8))
                + DELIMITER
                + event.timestamp();
    }

    static AuditEvent deserialize(String value) {
        String[] parts = value.split(DELIMITER, 3);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Некорректное событие аудита: " + value);
        }

        String id = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        return new AuditEvent(parts[0], id, Long.parseLong(parts[2]));
    }
}
