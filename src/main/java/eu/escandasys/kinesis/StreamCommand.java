package eu.escandasys.kinesis;

import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameRenderer;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.StreamSummary;
import software.amazon.awssdk.services.kinesisvideo.KinesisVideoClient;
import software.amazon.awssdk.services.kinesisvideo.model.*;
import software.amazon.awssdk.services.kinesisvideomedia.KinesisVideoMediaClient;
import software.amazon.awssdk.services.kinesisvideomedia.endpoints.KinesisVideoMediaEndpointProvider;
import software.amazon.awssdk.services.kinesisvideomedia.model.GetMediaRequest;
import software.amazon.awssdk.services.kinesisvideomedia.model.GetMediaResponse;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelectorType;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
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
        } else {
            Instant now = timestampSupplier.get();
            Instant startTime = now.minus(duration);
            log.info("Starting time at %s".formatted(startTime));
            startSelector = StartSelector.builder()
                    .startTimestamp(startTime)
                    .startSelectorType(StartSelectorType.PRODUCER_TIMESTAMP)
                    .build();
        }
        var httpClient = ApacheHttpClient.create();
        try (var engine = new StreamingEngine(httpClient, timestampSupplier)) {
            var streamOpt = engine.findStreamInfo(streamNameStr, streamArnStr);
            if (streamOpt.isEmpty()) {
                log.warn("No stream found for stream name %s".formatted(streamNameStr));
            } else {
                var stream = streamOpt.get();
                log.info("Found stream %s by ARN %s".formatted(stream.streamName(), stream.streamARN()));
                engine.pipe(duration, stream, startSelector, System.out);
            }
        }
    }
}
