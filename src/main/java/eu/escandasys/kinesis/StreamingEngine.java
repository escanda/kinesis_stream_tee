package eu.escandasys.kinesis;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jboss.logging.Logger;

import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameRenderer;

import software.amazon.awssdk.services.kinesisvideo.model.StreamInfo;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;

public class StreamingEngine {
    private static final Logger log = Logger.getLogger(StreamingEngine.class);
    
    private final KinesisRepository kinesisRepository;
    private final Supplier<Instant> timestampSupplier;
            
    public StreamingEngine(KinesisRepository repository, Supplier<Instant> timestampSupplier) {
        this.kinesisRepository = repository;
        this.timestampSupplier = timestampSupplier;
    }

    public Optional<StreamInfo> findStreamInfo(String streamNameStr, String streamArnStr) {
        if (Objects.isNull(streamNameStr) || Objects.isNull(streamArnStr)) {
            log.info("Searching through stream list for arn or name...");
            var streamInfos = kinesisRepository.streamInfos();
            log.info("Found %d stream names".formatted(streamInfos.size()));
            for (StreamInfo streamInfo : streamInfos) {
                log.info("Found stream summary for %s".formatted(streamInfo.streamName()));
                if (streamInfo.streamName().equalsIgnoreCase(streamNameStr)
                    || streamInfo.streamARN().equalsIgnoreCase(streamArnStr)) {
                    return Optional.of(streamInfo);
                }
            }
        }
        return Optional.empty();
    }

    public void pipe(Duration duration, StreamInfo stream, StartSelector startSelector, OutputStream os) throws IOException {
        final long cancelMs = duration.toMillis();
        try (var it = kinesisRepository.getMedia(startSelector, stream.streamName(), stream.streamARN())) {
            log.info("Reading input for stream with ARN %s".formatted(stream.streamARN()));
            final var start = timestampSupplier.get();
            var mkvTagProcessor = new FragmentMetadataVisitor.BasicMkvTagProcessor();
            Optional<FragmentMetadataVisitor.MkvTagProcessor> tagProcessor = Optional.of(mkvTagProcessor);
            final H264FrameRenderer frameProcessor = H264FrameRenderer.create(t -> this.onFrame(os, t));
            var visitor = FrameVisitor.create(frameProcessor, tagProcessor);
            log.info("Starting loop over mkv elements");
            while (it.hasNext()) {
                var element = measure("Retrieving one MKV element took %lMS", () -> it.next());
                @SuppressWarnings("unused")
                var _ignored = measure("Processing one frame took %lMS", () -> {
                    try {
                        element.accept(visitor);
                    } catch (MkvElementVisitException e) {
                        ExceptionUtils.rethrow(e);
                    }
                    return null;
                });
                var now = timestampSupplier.get();
                var timeDelta = start.until(now, ChronoUnit.MILLIS);
                if (timeDelta >= cancelMs) {
                    break;
                }
            }
        }
    }

    private <U> U measure(String fmt, Supplier<U> supplier) {
        var start = timestampSupplier.get();
        U i = null;
        try {
            i = supplier.get();
            var end = timestampSupplier.get();
            var ms = start.until(end, ChronoUnit.MILLIS);
            log.info(fmt.formatted(ms));
        } catch (Throwable t) {
            var end = timestampSupplier.get();
            log.error(fmt.formatted(start.until(end, ChronoUnit.MILLIS)));
            throw t;
        }
        return i;
    }

    public void onFrame(OutputStream os, BufferedImage bufferedImage) {
        log.info("writing rasterized to output");
        var raster = bufferedImage.getData();
        var size = raster.getDataBuffer().getSize();
        log.info("buffered image of size %d".formatted(size));
        var channel = Channels.newChannel(os);
        var longByteBuffer = ByteBuffer.allocate(8);
        longByteBuffer.putLong(size);
        log.info("wrote size to output");
        try {
            channel.write(longByteBuffer);
            DataBufferByte bufferBytes = (DataBufferByte) raster.getDataBuffer();
            byte[] bytes = bufferBytes.getData();
            var buffer = ByteBuffer.wrap(bytes);
            log.info("writing buffer to output of size %d".formatted(bytes.length));
            channel.write(buffer);
            log.info("wrote output");
        } catch (IOException e) {
            log.error("Cannot write to output channel", e);
        }
    }
}
