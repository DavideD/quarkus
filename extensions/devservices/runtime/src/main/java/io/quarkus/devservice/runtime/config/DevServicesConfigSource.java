package io.quarkus.devservice.runtime.config;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.spi.ConfigSource;

import io.quarkus.devservices.crossclassloader.runtime.RunningDevServicesRegistry;
import io.quarkus.devservices.crossclassloader.runtime.RunningService;
import io.quarkus.runtime.LaunchMode;

public class DevServicesConfigSource implements ConfigSource {

    private final LaunchMode launchMode;

    public DevServicesConfigSource(LaunchMode launchMode) {
        this.launchMode = launchMode;
    }

    @Override
    public Set<String> getPropertyNames() {
        Set<String> names = new HashSet<>();

        Set<RunningService> allConfig = RunningDevServicesRegistry.INSTANCE.getAllRunningServices(launchMode.name());
        if (allConfig != null) {
            for (RunningService service : allConfig) {
                Map<String, String> config = service.configs();
                names.addAll(config.keySet());
            }
        }
        return names;
    }

    @Override
    public String getValue(String propertyName) {
        Set<RunningService> allConfig = RunningDevServicesRegistry.INSTANCE.getAllRunningServices(launchMode.name());
        if (allConfig != null) {
            for (RunningService service : allConfig) {
                Map<String, String> config = service.configs();
                String answer = config.get(propertyName);
                if (answer != null) {
                    return answer;
                }
            }
        }
        return null;
    }

    @Override
    public String getName() {
        return "DevServicesConfigSource";
    }

    @Override
    public int getOrdinal() {
        return 240; // a bit less than application properties, because this config should only take effect if nothing is set
    }
}
