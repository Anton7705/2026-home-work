package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;

public class AuditServiceFactorySimple extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new AuditServiceSimple(bootstrapServers, consumerGroupId);
    }
}
