package eu.escandasys.kinesis;

import java.util.Collection;

import com.amazonaws.kinesisvideo.parser.mkv.MkvElement;

import software.amazon.awssdk.services.kinesisvideo.model.StreamInfo;
import software.amazon.awssdk.services.kinesisvideomedia.model.StartSelector;

public interface KinesisRepository {
    Collection<StreamInfo> streamInfos();
    ClosingIterator<MkvElement> getMedia(StartSelector startSelector, String streamName, String streamARN);
}
