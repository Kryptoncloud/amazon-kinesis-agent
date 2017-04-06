/*
 * Copyright (c) 2014-2016 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;
import org.apache.commons.lang3.RandomUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static org.mockito.Mockito.when;

public class KinesisRecordTest extends TestBase {
    @SuppressWarnings("rawtypes")
    private FileFlow flow;
    private TrackedFile file;

    @BeforeMethod
    public void setup() throws IOException {
        flow = Mockito.mock(KinesisFileFlow.class);
        when(((KinesisFileFlow) flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.RANDOM);
        when(flow.getRecordTerminatorBytes()).thenReturn(KinesisFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR.getBytes(StandardCharsets.UTF_8));
        file = Mockito.mock(TrackedFile.class);
        when(file.getFlow()).thenReturn(flow);
    }

    @Test
    public void testStartEndOffset() {
        KinesisRecord record = new KinesisRecord(file, 1023, new byte[100]);
        Assert.assertEquals(record.startOffset(), 1023);
        Assert.assertEquals(record.endOffset(), 1123);
    }

    @Test
    public void testRecordLength() {
        KinesisRecord record = new KinesisRecord(file, 1023, new byte[200]);
        String partitionKey = record.partitionKey();
        Assert.assertEquals(record.lengthWithOverhead(), 200 + partitionKey.length() + KinesisConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), 200 + partitionKey.length());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testTruncate() throws IOException {
        byte[] data = RandomUtils.nextBytes((KinesisConstants.MAX_RECORD_SIZE_BYTES) + RandomUtils.nextInt(1, 100));
        KinesisRecord record = new KinesisRecord(file, 1023, data);
        record.truncate();
        Assert.assertEquals(record.lengthWithOverhead(), KinesisConstants.MAX_RECORD_SIZE_BYTES + KinesisConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), KinesisConstants.MAX_RECORD_SIZE_BYTES);
        Assert.assertTrue(ByteBuffers.toString(record.data, StandardCharsets.UTF_8).endsWith(KinesisFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testGenerateDeterministicPartitionKey() {
        final PartitionKeyOption partitionKeyOption = KinesisConstants.PartitionKeyOption.DETERMINISTIC;
        when(((KinesisFileFlow) flow).getPartitionKeyOption()).thenReturn(partitionKeyOption);

        byte[] data = RandomUtils.nextBytes(200);
        KinesisRecord record = new KinesisRecord(file, 1023, data);
        Assert.assertNotNull(record.partitionKey());
        Assert.assertEquals(record.partitionKey(), record.generatePartitionKey(partitionKeyOption));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testGeneratePatternPartitionKey() {
        final PartitionKeyOption partitionKeyOption = PartitionKeyOption.PATTERN;
        when(((KinesisFileFlow) flow).getPartitionKeyOption()).thenReturn(partitionKeyOption);
        final Pattern pattern = Pattern.compile(".*\"deviceId\"\\s*:\\s*\"([a-zA-Z0-9-]+)\".*");
        when(((KinesisFileFlow) flow).getPartitionKeyPattern()).thenReturn(pattern);

        byte[] data = "{ \"deviceId\": \"1234-5678\", \"weight\": 52 }".getBytes();
        KinesisRecord record = new KinesisRecord(file, 1023, data);
        Assert.assertNotNull(record.partitionKey());
        Assert.assertEquals(record.partitionKey(), "1234-5678");
        Assert.assertEquals(record.partitionKey(), record.generatePartitionKey(partitionKeyOption));
    }
}
