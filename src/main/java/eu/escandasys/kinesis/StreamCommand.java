package eu.escandasys.kinesis;

import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

@Command
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

    private final Supplier<Instant> timestampSupplier = Instant::now;

    @Override
    public void run() {
        log.info("Variables -> Stream name: %s\tStart: %s\tDuration: %s\tFrames: %s".formatted(streamNameStr, startWhenceStr, durationStr, framesPerSecondStr));

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

        SdkHttpClient httpClient = ApacheHttpClient.create();
        try (KinesisClient kinesisClient = KinesisClient.builder()
                .httpClient(httpClient)
                .build()) {
            if (Objects.isNull(streamArnStr) || Objects.isNull(streamNameStr)) {
                ListStreamsResponse resp = kinesisClient.listStreams();
                for (StreamSummary streamSummary : resp.streamSummaries()) {
                    log.info("Found stream summary for %s".formatted(streamSummary.streamName()));
                    if (streamSummary.streamName().equalsIgnoreCase(streamNameStr)) {
                        streamArnStr = streamSummary.streamARN();
                        break;
                    }
                    if (streamSummary.streamARN().equalsIgnoreCase(streamArnStr)) {
                        streamNameStr = streamSummary.streamName();
                        break;
                    }
                }
            }
            if (Objects.isNull(streamArnStr)) {
                log.warn("No stream found for stream name %s".formatted(streamNameStr));
            } else {
                log.info("Found stream %s by ARN %s".formatted(streamNameStr, streamArnStr));

                try (KinesisVideoClient videoClient = KinesisVideoClient.builder()
                        .httpClient(httpClient)
                        .build()) {
                    GetDataEndpointResponse response = videoClient.getDataEndpoint(GetDataEndpointRequest.builder()
                            .streamName(streamNameStr)
                            .streamARN(streamArnStr)
                            .apiName(APIName.GET_MEDIA)
                            .build()
                    );
                    try (final KinesisVideoMediaClient kinesisVideoMediaClient = KinesisVideoMediaClient.builder()
                            .endpointOverride(URI.create(response.dataEndpoint()))
                            .endpointProvider(KinesisVideoMediaEndpointProvider.defaultProvider())
                            .httpClient(httpClient)
                            .build();
                         final ResponseInputStream<GetMediaResponse> mediaResponseResponseInputStream = kinesisVideoMediaClient.getMedia(GetMediaRequest.builder()
                                 .streamARN(streamArnStr)
                                 .startSelector(startSelector)
                                 .build());
                         final BufferedOutputStream bos = new BufferedOutputStream(System.out)) {
                        final Instant start = timestampSupplier.get();
                        Instant now;
                        do {
                            now = timestampSupplier.get();
                            int b = mediaResponseResponseInputStream.read();
                            if (b == -1) {
                                break;
                            }
                            bos.write(b);
                            bos.flush();
                        } while ((now.toEpochMilli() - start.toEpochMilli()) < duration.toMillis());
                    } catch (IOException e) {
                        log.error("Cannot read media with stream name %s and ARN %s".formatted(streamNameStr, streamArnStr), e);
                    }
                }
            }
        }
    }
}
