package eu.escandasys.kinesis;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.jboss.logging.Logger;

import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.FrameVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.H264FrameRenderer;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesisvideo.KinesisVideoClient;
import software.amazon.awssdk.services.kinesisvideo.model.APIName;
import software.amazon.awssdk.services.kinesisvideo.model.GetDataEndpointRequest;
import software.amazon.awssdk.services.kinesisvideo.model.GetDataEndpointResponse;
import software.amazon.awssdk.services.kinesisvideo.model.StreamInfo;
import software.amazon.awssdk.services.kinesisvideomedia.KinesisVideoMediaClient;
import software.amazon.awssdk.services.kinesisvideomedia.endpoints.KinesisVideoMediaEndpointProvider;
import software.amazon.awssdk.services.kinesisvideomedia.model.GetMediaRequest;
import software.amazon.awssdk.services.kinesisvideomedia.model.GetMediaResponse;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;

public class StreamingEngine implements AutoCloseable {
    private static final Logger log = Logger.getLogger(StreamingEngine.class);

    private final SdkHttpClient sdkHttpClient;
    private final KinesisClient kinesisClient;
    private final KinesisVideoClient videoClient;
    
    private final Supplier<Instant> timestampSupplier;
            
    public StreamingEngine(SdkHttpClient httpClient, Supplier<Instant> timestampSupplier) {
        this.sdkHttpClient = httpClient;
        this.kinesisClient = KinesisClient.builder()
            .httpClient(httpClient)
            .build();
        this.videoClient = KinesisVideoClient.builder()
            .httpClient(httpClient)
            .build();
        this.timestampSupplier = timestampSupplier;
    }

    @Override
    public void close() {
        kinesisClient.close();
        videoClient.close();
        sdkHttpClient.close();
    }

    public Optional<StreamInfo> findStreamInfo(String streamNameStr, String streamArnStr) {
        if (Objects.isNull(streamNameStr) || Objects.isNull(streamArnStr)) {
            log.info("Searching through stream list for arn or name...");
            var streams = videoClient.listStreams();
            log.info("Found %d stream names".formatted(streams.streamInfoList().size()));
            for (StreamInfo streamInfo : streams.streamInfoList()) {
                log.info("Found stream summary for %s".formatted(streamInfo.streamName()));
                if (streamInfo.streamName().equalsIgnoreCase(streamNameStr)
                    || streamInfo.streamARN().equalsIgnoreCase(streamArnStr)) {
                    return Optional.of(streamInfo);
                }
            }
        }
        return Optional.empty();
    }

    public void pipe(Duration duration, StreamInfo stream, StartSelector startSelector, OutputStream os) {
        GetDataEndpointResponse response = videoClient.getDataEndpoint(GetDataEndpointRequest.builder()
                .streamARN(stream.streamARN())
                .apiName(APIName.GET_MEDIA)
                .build()
        );
        try (final KinesisVideoMediaClient kinesisVideoMediaClient = KinesisVideoMediaClient.builder()
                .endpointOverride(URI.create(response.dataEndpoint()))
                .endpointProvider(KinesisVideoMediaEndpointProvider.defaultProvider())
                .httpClient(sdkHttpClient)
                .build();
            final ResponseInputStream<GetMediaResponse> is = kinesisVideoMediaClient.getMedia(GetMediaRequest.builder()
                .streamARN(stream.streamARN())
                .startSelector(startSelector)
                .build())) {
            log.info("Reading input for stream with ARN %s".formatted(stream.streamARN()));
            StreamingMkvReader mkvReader = StreamingMkvReader.createDefault(new InputStreamParserByteSource(is));
            log.info("Created MKV reader");
            final Instant start = timestampSupplier.get();
            Optional<FragmentMetadataVisitor.MkvTagProcessor> tagProcessor = Optional.of(new FragmentMetadataVisitor.BasicMkvTagProcessor());
            var frameProcessor = H264FrameRenderer.create(t -> this.onFrame(os, t));
            var visitor = FrameVisitor.create(frameProcessor, tagProcessor);
            while (mkvReader.mightHaveNext()) {
                var mkvOpt = mkvReader.nextIfAvailable();
                if (mkvOpt.isPresent()) {
                    log.debug("Reading next MKV frame");
                    var mkvElement = mkvOpt.get();
                    try {
                        mkvElement.accept(visitor);
                    } catch (MkvElementVisitException e) {
                        log.warn("Cannot visit MKV element", e);
                    }
                }
            } while ((timestampSupplier.get().toEpochMilli() - start.toEpochMilli()) < duration.toMillis());
        } catch (IOException e) {
            log.error("Cannot read media with stream name %s and ARN %s".formatted(stream.streamName(), stream.streamARN()), e);
        }
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
            log.info("Wrote output");
        } catch (IOException e) {
            log.error("Cannot write to output channel", e);
        }
    }
}
