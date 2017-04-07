/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class KinesisRecord extends AbstractRecord {
    protected final Pattern partitionKeyPattern;
    protected final String partitionKey;

    public KinesisRecord(TrackedFile file, long offset, ByteBuffer data) {
        super(file, offset, data);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow)file.getFlow();
        partitionKeyPattern = flow.getPartitionKeyPattern();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption());
    }

    public KinesisRecord(TrackedFile file, long offset, byte[] data) {
        super(file, offset, data);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow)file.getFlow();
        partitionKeyPattern = flow.getPartitionKeyPattern();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption());
    }
    
    public String partitionKey() {
        return partitionKey;
    }

    @Override
    public long lengthWithOverhead() {
        return length() + KinesisConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    public long length() {
        return dataLength() + partitionKey.length();
    }
    
    @Override
    protected int getMaxDataSize() {
        return KinesisConstants.MAX_RECORD_SIZE_BYTES - partitionKey.length();
    }
    
    @VisibleForTesting
    String generatePartitionKey(PartitionKeyOption option) {
        Preconditions.checkNotNull(option);
        
        if (option == PartitionKeyOption.DETERMINISTIC) {
            Hasher hasher = Hashing.md5().newHasher();
            hasher.putBytes(data.array());
            return hasher.hash().toString();
        }
        if (option == PartitionKeyOption.PATTERN) {
            String strData = new String(data.array());
            Matcher matcher = partitionKeyPattern.matcher(strData);
            if (matcher.matches() && matcher.groupCount() == 1) {
                return matcher.group(1);
            }
            throw new IllegalStateException("No matches");
        }
        if (option == PartitionKeyOption.RANDOM)
            return "" + ThreadLocalRandom.current().nextDouble(1000000);

        return null;
    }
}
