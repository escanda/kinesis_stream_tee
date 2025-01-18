package eu.escandasys.kinesis;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelectorType;

@QuarkusTest
public class StreamingEngineTest {
    @Inject
    Logger log;

    @Test
    public void testBasicInvocation() throws IOException {
        final Supplier<Instant> timestampSupplier = Instant::now;
        int framesPerSecond = Integer.parseInt("2");
        int timeBetweenFrames = (int) (1000.0d / framesPerSecond);
        Duration duration = Duration.parse("PT1M");
        log.info("Capturing frames every %sms.".formatted(timeBetweenFrames));
        log.info("Capturing since %ss ago".formatted(duration.toSeconds()));

        final var startSelector = StartSelector.builder()
            .startSelectorType(StartSelectorType.NOW)
            .build();

        String streamNameStr = "esys-casa-hall";

        try (var httpClient = ApacheHttpClient.create()) {
            var repository = new DefaultKinesisRepository(httpClient, timestampSupplier);
            var engine = new StreamingEngine(repository, timestampSupplier);
            var streamOpt = engine.findStreamInfo(streamNameStr, null);
            assertNotEquals(false, streamOpt.isPresent(), "cannot find stream in remote");
            var stream = streamOpt.get();
            var f = Files.createTempFile("kinesis-stream-tee", ".out");
            try (var os = new FileOutputStream(f.toFile())) {
                engine.pipe(duration, stream.streamName(), stream.streamARN(), startSelector, os);
            }
        }
    }
}
