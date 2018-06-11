/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.db.streaming.CassandraStreamReader.StreamDeserializer;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * InputStream which reads data from underlining source with given {@link CompressionInfo}. Uses {@link #buffer} as a buffer
 * for uncompressed data (which is read by stream consumers - {@link StreamDeserializer} in this case).
 */
public class CompressedInputStream extends RebufferingInputStream implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedInputStream.class);
    private static final int CHECKSUM_LENGTH = 4;

    /**
     * Indicates there was a problem when reading from source stream.
     * When this is added to the {@link #dataBuffer} by the stream {@link Reader},
     * it is expected that the {@link #readException} variable is populated
     * with the cause of the error when reading from source stream, so it is
     * thrown to the consumer on subsequent read operation.
     */
    private static final ByteBuffer POISON_PILL = ByteBuffer.wrap(new byte[0]);

    /**
     * A queue of the buffers as they come in off the wire.
     */
    private final BlockingQueue<ByteBuffer> dataBuffer;

    private final CompressionInfo info;
    private final DoubleSupplier crcCheckChanceSupplier;
    private final ChecksumType checksumType;

    /**
     * Reference to the background thread that will execute {{CompressedInputStream#Reader}},
     * so we can close it properly.
     */
    private final Thread readerThread;

    /**
     * Flag to the {@link #readerThread} if it should continue eecution.
     */
    private volatile boolean running;

    /**
     * The base offset of the current {@link #buffer} from the beginning of the file being streamed.
     */
    private long streamOffset = 0;

    /**
     * The current {@link CompressedCassandraStreamReader#sections} offset in the stream.
     */
    private long current = 0;

    /**
     * If an error occurs in while reading the buffers off the wire (in {@link Reader}), any exception is captured
     * in this field.
     */
    private volatile IOException readException = null;

    private long totalCompressedBytesRead;

    /**
     * Offset in current {@lilnk #buffer} at which reading began.
     */
    private int currentBufferOffset;

    /**
     * @param source Input source to read compressed data from
     * @param info Compression info
     */
    public CompressedInputStream(DataInputPlus source, CompressionInfo info, ChecksumType checksumType, DoubleSupplier crcCheckChanceSupplier)
    {
        super(ByteBuffer.allocateDirect(info.parameters.chunkLength()));
        buffer.limit(buffer.position()); // force the buffer to appear "consumed" so that it triggers reBuffer on the first read
        this.info = info;
        this.dataBuffer = new ArrayBlockingQueue<>(Math.min(info.chunks.length, 1024));
        this.crcCheckChanceSupplier = crcCheckChanceSupplier;
        this.checksumType = checksumType;

        running = true;
        readerThread = new FastThreadLocalThread(new Reader(source, info, dataBuffer));
        readerThread.start();
    }

    /**
     * Invoked when crossing into the next stream boundary in {@link CompressedCassandraStreamReader#sections}.
     */
    public void position(long position) throws IOException
    {
        if (readException != null)
            throw readException;

        assert position >= current : String.format("stream can only read forward; next position=%d, current position=%d", position, current);
        current = position;

        if (current > streamOffset + buffer.limit())
            reBuffer(false);

        currentBufferOffset = Ints.checkedCast(current - streamOffset);
        buffer.position(currentBufferOffset);
    }

    protected void reBuffer() throws IOException
    {
        reBuffer(true);
    }

    private void reBuffer(boolean updateCurrent) throws IOException
    {
        if (readException != null)
        {
            FileUtils.clean(buffer);
            buffer = null;
            throw readException;
        }

        // increment the offset into the stream based on the current buffer's read count
        if (updateCurrent)
            current += buffer.position() - currentBufferOffset;

        try
        {
            ByteBuffer compressedWithCRC = dataBuffer.take();
            if (compressedWithCRC == POISON_PILL)
            {
                assert readException != null;
                throw readException;
            }

            decompress(compressedWithCRC);
        }
        catch (InterruptedException e)
        {
            throw new EOFException("No chunk available");
        }
    }

    /**
     * Decompress the input buffer into either the reusable member {@link #buffer},
     * or simply use the input buffer if it is uncompressed.
     *
     * Note: the CRC is the last {@link #CHECKSUM_LENGTH} bytes of the buffer.
     */
    private void decompress(ByteBuffer compressed) throws IOException
    {
        int length = compressed.remaining();

        // uncompress if the buffer size is less than the max chunk size.
        // else, if the buffer size is greater than or equal to the maxCompressedLength,
        // we assume the buffer is not compressed. see CASSANDRA-10520
        if (length - CHECKSUM_LENGTH < info.parameters.maxCompressedLength())
        {
            buffer.clear();
            compressed.limit(length - CHECKSUM_LENGTH);
            info.parameters.getSstableCompressor().uncompress(compressed, buffer);
            buffer.flip();
            validateCRC(compressed, length);
            FileUtils.clean(compressed);
        }
        else
        {
            FileUtils.clean(buffer);
            buffer = compressed;
            buffer.limit(length - CHECKSUM_LENGTH);
            validateCRC(compressed, length);
        }
        totalCompressedBytesRead += length;
        currentBufferOffset = 0;

        // buffer offset is always aligned
        final int compressedChunkLength = info.parameters.chunkLength();
        streamOffset = current & ~(compressedChunkLength - 1);
    }

    private void validateCRC(ByteBuffer compressed, int length) throws IOException
    {
        double crcCheckChance = this.crcCheckChanceSupplier.getAsDouble();
        if (crcCheckChance >= 1d ||
            (crcCheckChance > 0d && crcCheckChance > ThreadLocalRandom.current().nextDouble()))
        {
            ByteBuffer crcBuf = compressed.duplicate();
            crcBuf.limit(length - CHECKSUM_LENGTH).position(0);
            int checksum = (int) checksumType.of(crcBuf);

            crcBuf.limit(length);
            if (crcBuf.getInt() != checksum)
                throw new IOException("CRC unmatched");
        }
    }

    long getTotalCompressedBytesRead()
    {
        return totalCompressedBytesRead;
    }

    /**
     * {@inheritDoc}
     *
     * Releases the resources specific to this instance, but not the {@link DataInputPlus} that is used by the {@link Reader}.
     */
    @Override
    public void close()
    {
        if (running)
        {
            readException = new ClosedChannelException();

            if (buffer != null)
            {
                FileUtils.clean(buffer);
                buffer = null;
            }

            for (ByteBuffer buf : dataBuffer)
                FileUtils.clean(buf);

            running = false;
        }
    }

    class Reader extends WrappedRunnable
    {
        private final DataInputPlus source;
        private final Iterator<CompressionMetadata.Chunk> chunks;
        private final BlockingQueue<ByteBuffer> dataBuffer;

        Reader(DataInputPlus source, CompressionInfo info, BlockingQueue<ByteBuffer> dataBuffer)
        {
            this.source = source;
            this.chunks = Iterators.forArray(info.chunks);
            this.dataBuffer = dataBuffer;
        }

        protected void runMayThrow() throws Exception
        {
            byte[] tmp = null;
            boolean isReadableByteChannel = source instanceof ReadableByteChannel;
            while (chunks.hasNext())
            {
                if (!running)
                    return;

                CompressionMetadata.Chunk chunk = chunks.next();

                int readLength = chunk.length + 4; // read with CRC
                ByteBuffer compressedWithCRC = null;
                try
                {
                    final int r;
                    if (isReadableByteChannel)
                    {
                        compressedWithCRC = ByteBuffer.allocateDirect(readLength);
                        r = ((ReadableByteChannel)source).read(compressedWithCRC);
                        compressedWithCRC.flip();
                    }
                    else
                    {
                        // read into an on-heap araay, then copy over to an off-heap buffer. at a minumum snappy requires
                        // off-heap buffers for decompression, else we could have just wrapped the plain byte array in a ByteBuffer
                        if (tmp == null || tmp.length < info.parameters.chunkLength() + CHECKSUM_LENGTH)
                            tmp = new byte[info.parameters.chunkLength() + CHECKSUM_LENGTH];
                        source.readFully(tmp, 0, readLength);
                        compressedWithCRC = ByteBuffer.allocateDirect(readLength);
                        compressedWithCRC.put(tmp, 0, readLength);
                        compressedWithCRC.position(0);
                        r = readLength;
                    }

                    if (r < 0)
                    {
                        FileUtils.clean(compressedWithCRC);
                        readException = new EOFException("No chunk available");
                        dataBuffer.put(POISON_PILL);
                        return; // throw exception where we consume dataBuffer
                    }
                }
                catch (IOException e)
                {
                    if (!(e instanceof EOFException))
                        logger.warn("Error while reading compressed input stream.", e);
                    if (compressedWithCRC != null)
                        FileUtils.clean(compressedWithCRC);

                    readException = e;
                    dataBuffer.put(POISON_PILL);
                    return; // throw exception where we consume dataBuffer
                }
                dataBuffer.put(compressedWithCRC);
            }
        }
    }
}
