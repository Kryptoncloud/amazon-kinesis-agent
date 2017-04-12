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

import com.amazon.kinesis.streaming.agent.Logging;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

public class KinesisRecord extends AbstractRecord {
    private static final Logger LOGGER = Logging.getLogger(KinesisRecord.class);
    /**
     * The latest time in millis when no matches were found for PATTERN based partition key
     */
    private static final AtomicLong lastWarningTime = new AtomicLong();
    /**
     * Backoff time in millis in order not to overflow log files with warning messages
     */
    private static final long backoffMillis = 60000;

    protected final String partitionKey;

    public KinesisRecord(TrackedFile file, long offset, ByteBuffer data) {
        super(file, offset, data);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow) file.getFlow();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption());
    }

    public KinesisRecord(TrackedFile file, long offset, byte[] data) {
        super(file, offset, data);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow) file.getFlow();
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
            KinesisFileFlow flow = (KinesisFileFlow) file.getFlow();
            Matcher matcher = flow.getPartitionKeyPattern().matcher(getRemainingString());
            if (matcher.matches() && matcher.groupCount() == 1) {
                return matcher.group(1);
            } else {
                return generatePartitionKeyWithFallback(flow.getPartitionKeyFallbackOption());
            }
        }
        if (option == PartitionKeyOption.RANDOM)
            return "" + ThreadLocalRandom.current().nextDouble(1000000);

        return null;
    }

    private String generatePartitionKeyWithFallback(PartitionKeyOption fallbackOption) {
        long curMillis = System.currentTimeMillis();
        long lastWarnMillis = lastWarningTime.get();
        if (curMillis - lastWarnMillis > backoffMillis) {
            boolean isUpdated = lastWarningTime.compareAndSet(lastWarnMillis, curMillis);
            if (isUpdated) {
                LOGGER.error("Line [" + getRemainingString() + "] didn't match with pattern specified." +
                        " Skipping errors for " + backoffMillis + "ms");
            }
        }
        return generatePartitionKey(fallbackOption);
    }

    private String getRemainingString() {
        byte[] bytes = Arrays.copyOfRange(data.array(), data.arrayOffset(), data.arrayOffset() + data.remaining());
        return new String(bytes, 0, bytes.length).trim();
    }
}
