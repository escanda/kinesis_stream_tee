/*
Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). 
You may not use this file except in compliance with the License. 
A copy of the License is located at

   http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. 
This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
*/
package com.amazonaws.kinesisvideo.parser.utilities;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * Container class for H264 encoded frame
 */
@Getter
@Builder
public class EncodedFrame {

    private final ByteBuffer byteBuffer;
    private final ByteBuffer cpd;
    private final boolean isKeyFrame;
    @Setter
    private long timeCode;

}
