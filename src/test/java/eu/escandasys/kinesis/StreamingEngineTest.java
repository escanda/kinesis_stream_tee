package eu.escandasys.kinesis;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import java.io.ByteArrayOutputStream;
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
    public void testBasicInvocation() {
        final Supplier<Instant> timestampSupplier = Instant::now;
        int framesPerSecond = Integer.parseInt("2");
        int timeBetweenFrames = (int) (1000.0d / framesPerSecond);
        Duration duration = Duration.parse("PT1M");
        log.info("Capturing frames every %sms.".formatted(timeBetweenFrames));
        log.info("Capturing since %ss ago".formatted(duration.toSeconds()));

        final StartSelector startSelector = StartSelector.builder()
            .startSelectorType(StartSelectorType.NOW)
            .build();
        var httpClient = ApacheHttpClient.create();

        String streamNameStr = "esys-casa-hall";
        String streamArnStr = null;

        try (var engine = new StreamingEngine(httpClient, timestampSupplier)) {
            var streamOpt = engine.findStreamInfo(streamNameStr, streamArnStr);
            var stream = streamOpt.get();
            log.info("Found stream %s by ARN %s".formatted(stream.streamName(), stream.streamARN()));
            var baos = new ByteArrayOutputStream();
            engine.pipe(duration, stream, startSelector, baos);
            var arr = baos.toByteArray();
            assertNotEquals(0, arr.length);
        }
    }
}
