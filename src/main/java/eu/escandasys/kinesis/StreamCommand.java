package eu.escandasys.kinesis;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelectorType;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

@Command
@Dependent
public class StreamCommand implements Runnable {
    @Inject
    Logger log;

    @Option(names = {"-n ", "--stream-name"}, description = "Stream name.")
    String streamNameStr;

    @Option(names = {"-a ", "--stream-arn"}, description = "Stream ARN.")
    String streamArnStr;

    @Option(names = {"-t", "--start"}, description = "Whence to start from streaming data from archive", defaultValue = "now")
    String startWhenceStr;

    @Option(names = {"-d", "--duration"}, description = "Duration while streaming data into stdout", defaultValue = "PT0s")
    String durationStr;

    @Option(names = {"-f", "--frames-per-second"}, description = "Frames per second", defaultValue = "2")
    String framesPerSecondStr;

    @Override
    public void run() {
        final Supplier<Instant> timestampSupplier = Instant::now;
        int framesPerSecond = Integer.parseInt(framesPerSecondStr);
        int timeBetweenFrames = (int) (1000.0d / framesPerSecond);
        Duration duration = Duration.parse(durationStr);
        log.info("Capturing frames every %sms.".formatted(timeBetweenFrames));
        log.info("Capturing since %ss ago".formatted(duration.toSeconds()));
        final StartSelector startSelector;
        if (startWhenceStr.equalsIgnoreCase("now")) {
            startSelector = StartSelector.builder()
                    .startSelectorType(StartSelectorType.NOW)
                    .build();
        } else if (startWhenceStr.equalsIgnoreCase("earliest")) {
            startSelector = StartSelector.builder()
                .startSelectorType(StartSelectorType.EARLIEST)
                .build();
        } else {
            Instant now = timestampSupplier.get();
            Instant startTime = now.minus(duration);
            log.info("Starting time at %s".formatted(startTime));
            startSelector = StartSelector.builder()
                    .startTimestamp(startTime)
                    .startSelectorType(StartSelectorType.PRODUCER_TIMESTAMP)
                    .build();
        }
        try (var httpClient = ApacheHttpClient.create()) {
            var repository = new DefaultKinesisRepository(httpClient, timestampSupplier);
            var engine = new StreamingEngine(repository, timestampSupplier);
            var streamOpt = engine.findStreamInfo(streamNameStr, streamArnStr);
            if (streamOpt.isEmpty()) {
                log.warn("No stream found for stream name %s".formatted(streamNameStr));
            } else {
                var stream = streamOpt.get();
                log.info("Found stream %s by ARN %s".formatted(stream.streamName(), stream.streamARN()));
                engine.pipe(duration, stream.streamName(), stream.streamARN(), startSelector, System.out);
            }
        } catch (IOException e) {
            log.error("Cannot pipe entirely stream", e);
        }
    }
}
