package eu.escandasys.kinesis;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import com.amazonaws.kinesisvideo.parser.ebml.InputStreamParserByteSource;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElement;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.SdkHttpClient;
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

public class DefaultKinesisRepository implements KinesisRepository {
    private static final Logger log = Logger.getLogger(DefaultKinesisRepository.class);
    
    private static final int MAX_BUFFER_SIZE = 2048;
    
    private final SdkHttpClient sdkHttpClient;
    private final KinesisVideoClient videoClient;
    
    public DefaultKinesisRepository(SdkHttpClient httpClient, Supplier<Instant> timestampSupplier) {
        this.sdkHttpClient = httpClient;
        this.videoClient = KinesisVideoClient.builder()
            .httpClient(httpClient)
            .build();
    }

    @Override
    public ClosingIterator<MkvElement> getMedia(StartSelector startSelector, String streamName, String streamARN) {
        GetDataEndpointResponse response = videoClient.getDataEndpoint(GetDataEndpointRequest.builder()
                .streamARN(streamARN)
                .apiName(APIName.GET_MEDIA)
                .build());
        final KinesisVideoMediaClient kinesisVideoMediaClient = KinesisVideoMediaClient.builder()
            .endpointOverride(URI.create(response.dataEndpoint()))
            .endpointProvider(KinesisVideoMediaEndpointProvider.defaultProvider())
            .httpClient(sdkHttpClient)
            .build();
        final ResponseInputStream<GetMediaResponse> is = kinesisVideoMediaClient.getMedia(GetMediaRequest.builder()
            .streamARN(streamARN)
            .startSelector(startSelector)
            .build());
        final var isI = new InputStreamParserByteSource(is);
        final var mkvReader = StreamingMkvReader.createWithMaxContentSize(isI, MAX_BUFFER_SIZE);
        return new ClosingIterator<>(mkvReader::nextIfAvailable, () -> {
            try {
                is.close();
            } catch (IOException e) {
                log.error("Error closing response", e);
            }
            kinesisVideoMediaClient.close();
        });
    }

    public Collection<StreamInfo> streamInfos() {
        return Collections.unmodifiableCollection(videoClient.listStreams().streamInfoList());
    }
}
