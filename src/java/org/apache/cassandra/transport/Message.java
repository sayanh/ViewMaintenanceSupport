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
package org.apache.cassandra.transport;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import de.tum.viewmaintenance.config.ViewMaintenanceConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.viewmaintenance.ViewMaintenanceLogsReader;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.JVMStabilityInspector;

import javax.xml.validation.SchemaFactoryLoader;


/**
 * A message from the CQL binary protocol.
 */
public abstract class Message {
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);
    private static int operation_id = 1;

    /**
     * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages}
     * (because we have no better way to distinguish) and log them at DEBUG rather than INFO, since they
     * are generally caused by unclean client disconnects rather than an actual problem.
     */
    private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().
            add("Connection reset by peer").
            add("Broken pipe").
            add("Connection timed out").
            build();

    public interface Codec<M extends Message> extends CBCodec<M> {
    }

    public enum Direction {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection) {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion) {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type {
        ERROR(0, Direction.RESPONSE, ErrorMessage.codec),
        STARTUP(1, Direction.REQUEST, StartupMessage.codec),
        READY(2, Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE(3, Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS(4, Direction.REQUEST, CredentialsMessage.codec),
        OPTIONS(5, Direction.REQUEST, OptionsMessage.codec),
        SUPPORTED(6, Direction.RESPONSE, SupportedMessage.codec),
        QUERY(7, Direction.REQUEST, QueryMessage.codec),
        RESULT(8, Direction.RESPONSE, ResultMessage.codec),
        PREPARE(9, Direction.REQUEST, PrepareMessage.codec),
        EXECUTE(10, Direction.REQUEST, ExecuteMessage.codec),
        REGISTER(11, Direction.REQUEST, RegisterMessage.codec),
        EVENT(12, Direction.RESPONSE, EventMessage.codec),
        BATCH(13, Direction.REQUEST, BatchMessage.codec),
        AUTH_CHALLENGE(14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE(15, Direction.REQUEST, AuthResponse.codec),
        AUTH_SUCCESS(16, Direction.RESPONSE, AuthSuccess.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;

        static {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values()) {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        private Type(int opcode, Direction direction, Codec<?> codec) {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction) {
            if (opcode >= opcodeIdx.length)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                        t.direction,
                        direction,
                        opcode,
                        t));
            return t;
        }
    }

    public final Type type;
    protected Connection connection;
    private int streamId;
    private Frame sourceFrame;

    protected Message(Type type) {
        this.type = type;
    }

    public void attach(Connection connection) {
        this.connection = connection;
    }

    public Connection connection() {
        return connection;
    }

    public Message setStreamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId() {
        return streamId;
    }

    public void setSourceFrame(Frame sourceFrame) {
        this.sourceFrame = sourceFrame;
    }

    public Frame getSourceFrame() {
        return sourceFrame;
    }

    public static abstract class Request extends Message {
        protected boolean tracingRequested;

        protected Request(Type type) {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        public abstract Response execute(QueryState queryState);

        public void setTracingRequested() {
            this.tracingRequested = true;
        }

        public boolean isTracingRequested() {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message {
        protected UUID tracingId;

        protected Response(Type type) {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        public Message setTracingId(UUID tracingId) {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId() {
            return tracingId;
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame> {
        public void decode(ChannelHandlerContext ctx, Frame frame, List results) {
            boolean isRequest = frame.header.type.direction == Direction.REQUEST;
            boolean isTracing = frame.header.flags.contains(Frame.Header.Flag.TRACING);

            UUID tracingId = isRequest || !isTracing ? null : CBUtil.readUUID(frame.body);

            try {
                Message message = frame.header.type.codec.decode(frame.body, frame.header.version);
                message.setStreamId(frame.header.streamId);
                message.setSourceFrame(frame);

                if (isRequest) {
                    assert message instanceof Request;
                    Request req = (Request) message;
                    Connection connection = ctx.channel().attr(Connection.attributeKey).get();
                    req.attach(connection);
                    if (isTracing)
                        req.setTracingRequested();
                } else {
                    assert message instanceof Response;
                    if (isTracing)
                        ((Response) message).setTracingId(tracingId);
                }

                results.add(message);
            } catch (Throwable ex) {
                frame.release();
                // Remember the streamId
                throw ErrorMessage.wrap(ex, frame.header.streamId);
            }
        }
    }

    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Message> {
        public void encode(ChannelHandlerContext ctx, Message message, List results) {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
            int version = connection == null ? Server.CURRENT_VERSION : connection.getVersion();

            EnumSet<Frame.Header.Flag> flags = EnumSet.noneOf(Frame.Header.Flag.class);

            Codec<Message> codec = (Codec<Message>) message.type.codec;
            try {
                int messageSize = codec.encodedSize(message, version);
                ByteBuf body;
                if (message instanceof Response) {
                    UUID tracingId = ((Response) message).getTracingId();
                    if (tracingId != null) {
                        body = CBUtil.allocator.buffer(CBUtil.sizeOfUUID(tracingId) + messageSize);
                        CBUtil.writeUUID(tracingId, body);
                        flags.add(Frame.Header.Flag.TRACING);
                    } else {
                        body = CBUtil.allocator.buffer(messageSize);
                    }
                } else {
                    assert message instanceof Request;
                    body = CBUtil.allocator.buffer(messageSize);
                    if (((Request) message).isTracingRequested())
                        flags.add(Frame.Header.Flag.TRACING);
                }

                try {
                    codec.encode(message, body, version);
                } catch (Throwable e) {
                    body.release();
                    throw e;
                }

                results.add(Frame.create(message.type, message.getStreamId(), version, flags, body));
            } catch (Throwable e) {
                throw ErrorMessage.wrap(e, message.getStreamId());
            }
        }
    }

    @ChannelHandler.Sharable
    public static class Dispatcher extends SimpleChannelInboundHandler<Request> {
        private static class FlushItem {
            final ChannelHandlerContext ctx;
            final Object response;
            final Frame sourceFrame;

            private FlushItem(ChannelHandlerContext ctx, Object response, Frame sourceFrame) {
                this.ctx = ctx;
                this.sourceFrame = sourceFrame;
                this.response = response;
            }
        }

        private static final class Flusher implements Runnable {
            final EventLoop eventLoop;
            final ConcurrentLinkedQueue<FlushItem> queued = new ConcurrentLinkedQueue<>();
            final AtomicBoolean running = new AtomicBoolean(false);
            final HashSet<ChannelHandlerContext> channels = new HashSet<>();
            final List<FlushItem> flushed = new ArrayList<>();
            int runsSinceFlush = 0;
            int runsWithNoWork = 0;

            private Flusher(EventLoop eventLoop) {
                this.eventLoop = eventLoop;
            }

            void start() {
                if (!running.get() && running.compareAndSet(false, true)) {
                    this.eventLoop.execute(this);
                }
            }

            public void run() {

                boolean doneWork = false;
                FlushItem flush;
                while (null != (flush = queued.poll())) {
                    channels.add(flush.ctx);
                    flush.ctx.write(flush.response, flush.ctx.voidPromise());
                    flushed.add(flush);
                    doneWork = true;
                }

                runsSinceFlush++;

                if (!doneWork || runsSinceFlush > 2 || flushed.size() > 50) {
                    for (ChannelHandlerContext channel : channels)
                        channel.flush();
                    for (FlushItem item : flushed)
                        item.sourceFrame.release();

                    channels.clear();
                    flushed.clear();
                    runsSinceFlush = 0;
                }

                if (doneWork) {
                    runsWithNoWork = 0;
                } else {
                    // either reschedule or cancel
                    if (++runsWithNoWork > 5) {
                        running.set(false);
                        if (queued.isEmpty() || !running.compareAndSet(false, true))
                            return;
                    }
                }

                eventLoop.schedule(this, 10000, TimeUnit.NANOSECONDS);
            }
        }

        private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();

        public Dispatcher() {
            super(false);
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, Request request) {

            final Response response;
            final ServerConnection connection;

            try {
                assert request.connection() instanceof ServerConnection;
                connection = (ServerConnection) request.connection();
                QueryState qstate = connection.validateNewMessage(request.type, connection.getVersion(), request.getStreamId());

                logger.debug("Received: {}, v={}", request, connection.getVersion());

                response = request.execute(qstate);
                response.setStreamId(request.getStreamId());
                response.attach(connection);
                connection.applyStateTransition(request.type, response.type);

                // Capturing the entered the message for INSERT and UPDATE
                if (response.toString().equals("EMPTY RESULT") &&
                        (request.toString().toLowerCase().contains("insert") ||
                                request.toString().toLowerCase().contains("delete") ||
                                request.toString().toLowerCase().contains("update"))) {
                    // Assumption: The view table has columns with the string view
                    // TODO: Need to read from the view config.xml and exclude the requests for view maintenance activities
                    if (!request.toString().toLowerCase().contains("schema2.vt")) {
                        parseInputForViewMaintenance(request.toString());
                        ViewMaintenanceLogsReader.getInstance();
                    }
                }
            } catch (Throwable t) {
                JVMStabilityInspector.inspectThrowable(t);
                UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), true);
                flush(new FlushItem(ctx, ErrorMessage.fromException(t, handler).setStreamId(request.getStreamId()), request.getSourceFrame()));
                return;
            }

            logger.debug("Responding: {}, v={}", response, connection.getVersion());
            flush(new FlushItem(ctx, response, request.getSourceFrame()));
        }

        /*
        * Parses the input for insert and update queries only.
        * The input is converted to a JSON and saved for view maintenance process.
        * @param rawInput
        *
        */

        private void parseInputForViewMaintenance(String rawInput) {
            // Sample input string: QUERY INSERT INTO .....; We are ignoring the
            rawInput = rawInput.toLowerCase().substring("query ".length(), rawInput.length() - 1);

            BufferedWriter writer = null;
            logger.debug(" raw input string to process = " + rawInput);

            // Insert parsing..

//            sampleInsert = sampleInsert.toLowerCase().substring("query ".length(), sampleInsert.length() - 1);
//            sampleUpdate = sampleUpdate.toLowerCase().substring("query ".length(), sampleUpdate.length());
            logger.debug("modified string = " + rawInput);
            boolean isInsert = false;
            boolean isUpdate = false;
            boolean isColumn = false;
            boolean isValues = false;
            boolean isDelete = false;
            List<Object> whereSetDelete = new ArrayList<Object>();
            List<String> whereSetUpdate = new ArrayList<String>();
            StringTokenizer tokenizer = new StringTokenizer(rawInput, " ,");
            int count = 0;
            String tableName = "";
            String schemaName = "";
            List<String> columnSet = new ArrayList<String>();
            List<Object> dataSet = new ArrayList<Object>();
            Stack<Object> subListStack = new Stack<Object>();


            while (tokenizer.hasMoreTokens()) {
                String tempTokenStr = tokenizer.nextToken();
                if (tempTokenStr != null || !tempTokenStr.equals("")) {
                    System.out.println("token =" + tempTokenStr.trim());
                    if (count == 0) {
                        if (tempTokenStr.equals("insert")) {
                            isInsert = true;
                        } else if (tempTokenStr.equals("update")) {
                            isUpdate = true;
                        } else if (tempTokenStr.equals("delete")) {
                            isDelete = true;
                        }
                    }
                    if (isInsert) {
                        if (count == 2) {
                            tableName = tempTokenStr;
                        } else if (count == 3) {
                            if (tempTokenStr.equals("(")) {
                                isColumn = true;
                            } else if (tempTokenStr.substring(0, 1).equals("(")) {
                                if (tempTokenStr.substring(tempTokenStr.length() - 1).equals(")")) {
                                    System.out.println(" printing for one field starts and ends with parentheses " +
                                            tempTokenStr.substring(1, tempTokenStr.length() - 1));
                                    columnSet.add(tempTokenStr.substring(1, tempTokenStr.length() - 1));
                                } else {
                                    isColumn = true;
                                    columnSet.add(tempTokenStr.substring(1, tempTokenStr.length()));
                                }
                            }
                        }
                        if (isColumn && count > 3) {
                            System.out.println("testing = " + tempTokenStr.substring(tempTokenStr.length() - 1));
                            if (tempTokenStr.equals(")")) {
                                isColumn = false;
                            } else if (tempTokenStr.substring(tempTokenStr.length() - 1).equals(")")) {
                                isColumn = false;
                                columnSet.add(tempTokenStr.substring(0, tempTokenStr.length() - 1));
                            } else {
                                columnSet.add(tempTokenStr);
                            }
                        }
                        if (isValues) {
                            if (!tempTokenStr.equals("(")) {
                                if (tempTokenStr.equals(")")) {
                                    isValues = false;
                                } else if (tempTokenStr.substring(0, 1).equals("(")) {
                                    if (tempTokenStr.substring(tempTokenStr.length() - 1).equals(")")) {
                                        dataSet.add(tempTokenStr.substring(1, tempTokenStr.length() - 1));
                                        isValues = false;
                                    } else {
                                        dataSet.add(tempTokenStr.substring(1));
                                    }
                                } else if (tempTokenStr.substring(tempTokenStr.length() - 1).equals(")")) {
                                    dataSet.add(tempTokenStr.substring(0, tempTokenStr.length() - 1));
                                    isValues = false;
                                } else if (tempTokenStr.equals("{") || tempTokenStr.substring(0, 1).equals("{")) {
                                    logger.debug("Inside braces section ....");
                                    String tempSubListToken = "";
                                    int counterListsStart = 1;
                                    int counterListsEnd = 0;
                                    List<Object> tempSubList = new ArrayList<Object>();
                                    if (tempTokenStr.substring(0, 1).equals("{") && tempTokenStr.length() > 1) {
                                        subListStack.push("{");
                                        subListStack.push(tempTokenStr.substring(1, tempTokenStr.length()));
                                    } else {
                                        subListStack.push(tempTokenStr);
                                    }

                                    while (counterListsEnd < counterListsStart) {
                                        tempSubListToken = tokenizer.nextToken();
                                        logger.debug("temporary token = " + tempSubListToken);
                                        if (tempSubListToken.equals("}")) {
                                            counterListsEnd++;
                                            subListStack.push(tempSubListToken);
                                        } else if (tempSubListToken.equals("{")) {
                                            counterListsStart++;
                                            subListStack.push(tempSubListToken);
                                        } else if (tempSubListToken.substring(0, 1).equals("{")) {   // Case: , {'abc',....}
                                            counterListsStart++;
                                            subListStack.push("{");
                                            tempSubListToken = tempSubListToken.substring(1, tempSubListToken.length());
                                            while (tempSubListToken.substring(0, 1).equals("{")) {
                                                subListStack.push("{");
                                                counterListsStart++;
                                                tempSubListToken = tempSubListToken.substring(1, tempSubListToken.length());
                                            }
                                            while (tempSubListToken.substring(tempSubListToken.length() - 1).equals("}")) {
                                                subListStack.push("}");
                                                counterListsEnd++;
                                                tempSubListToken = tempSubListToken.substring(0, tempSubListToken.length() - 1);
                                            }
                                            subListStack.push(tempSubListToken);

                                        } else if (tempSubListToken.substring(tempSubListToken.length() - 1).equals("}")) {
                                            counterListsEnd++;
                                            Stack<Object> tempStack = new Stack<Object>();
                                            tempStack.push("}");
                                            tempSubListToken = tempSubListToken.substring(0, tempSubListToken.length() - 1);
                                            while (tempSubListToken.substring(0, 1).equals("{")) {
                                                subListStack.push("{");
                                                counterListsStart++;
                                                tempSubListToken = tempSubListToken.substring(1, tempSubListToken.length());
                                            }
                                            while (tempSubListToken.substring(tempSubListToken.length() - 1).equals("}")) {
                                                tempStack.push("}");
                                                counterListsEnd++;
                                                tempSubListToken = tempSubListToken.substring(0, tempSubListToken.length() - 1);
                                            }
                                            subListStack.push(tempSubListToken);
                                            while (!tempStack.isEmpty()) {
                                                subListStack.push(tempStack.pop());
                                            }
                                        } else {

                                            subListStack.push(tempSubListToken);
                                        }
                                    }
                                    logger.debug("contents of the stack ..." + subListStack.toString());
                                    logger.debug("INdex for this field = " + (dataSet.size()));
                                    logger.debug("Column name for this field = " + columnSet.get(dataSet.size()));
                                    JSONObject subDataObj = convertStackToJSON(columnSet.get(dataSet.size()), subListStack);
                                    logger.debug("sub data json is = " + subDataObj.toJSONString());
                                    dataSet.add(subDataObj.toJSONString());

                                } else {
                                    dataSet.add(tempTokenStr);
                                }

                            }
                        }

                        if (tempTokenStr.equals("values")) {
                            isColumn = false;
                            isValues = true;
                        }

                    }

                    // Extraction of elements for update queries like : Update schema1.users set column_name = new_value where primary_key = certain_value;
                    // Clean query with spaces after each word and special character
                    // Very sensitive parser to just one column update...can be extended later
                    if (isUpdate) {
                        logger.debug("This is an update process");
                        if (count == 1) {
                            tableName = tempTokenStr;
                        } else if (count == 3) {
                            columnSet.add(tempTokenStr);
                        } else if (count == 5) {
                            dataSet.add(tempTokenStr);
                        } else if (tempTokenStr.equals("where")) {
                            isColumn = true;
                        } else if(isColumn) {
                            whereSetUpdate.add(tempTokenStr);
                        }
                    }

                    // Extraction of elements for delete queries like : DELETE from schema1.users where user_id = 'TUMID10';
                    if (isDelete) {
                        logger.debug("This is a delete process");
                        if (count >= 1) {
                            if (!isColumn && !tempTokenStr.equals("from")) {
                                columnSet.add(tempTokenStr);
                            }
                            if (tempTokenStr.equals("from")) {
                                isColumn = true;
                                tableName = tokenizer.nextToken();
                            } else if (isColumn) {
                                whereSetDelete.add(tempTokenStr);
                            }
                        }
                    }
                    count++;
                }
            }

            logger.debug("column map is " + columnSet);
            logger.debug("values map is " + dataSet);
            if(isUpdate) {
                logger.debug("Update statement parsed = {} with data {} and where as {}" + columnSet, dataSet, whereSetUpdate );
            }
            if (tableName.contains("."))
            {
                String tempArr[] = tableName.split(".");
                if (tempArr.length == 2) {
                    schemaName = tempArr[0];
                    tableName = tempArr[1];
                }

            }
            logger.debug("table name = " + tableName);
            if (!schemaName.equals("")) {
                logger.debug("Schema from the query" + schemaName);
            } else {
                logger.debug("Schema name is not provided in the query");
            }
            logger.debug("type of query = " + (isInsert ? "insert" : "update"));

            // trying to get the table definition and structure

            List<String> ksDefList = Schema.instance.getNonSystemKeyspaces();
            for (String ksName : ksDefList) {
                logger.debug("non schemas are =" + ksName);
                if (ksName.equals("schema1")) {
                    // Have checks for the static keyspaces which we are interested in view maintenance
                    // I believe this will fetch all the non system keyspaces.
                    Map<String, CFMetaData> ksMetaDataMap = Schema.instance.getKeyspaceMetaData(ksName);
                    for (Map.Entry<String, CFMetaData> entry : ksMetaDataMap.entrySet()) {
                        String key = entry.getKey();
                        CFMetaData valueMetaData = entry.getValue();
                        Collection<ColumnDefinition> columnDefinitionList = valueMetaData.allColumns();
                        for (Iterator iterator = columnDefinitionList.iterator(); iterator.hasNext(); ) {
                            ColumnDefinition colDef = (ColumnDefinition) iterator.next();
                            logger.debug("Column to string = " + colDef.toString());
                            logger.debug("is partition key = " + colDef.isPartitionKey());
                            logger.debug("type = " + colDef.type);
                        }
                    }
                }

            }

            JSONObject jsonObject = null;

            if (isInsert) {
                jsonObject = convertRequestToJSON(tableName, columnSet, dataSet, "insert");
            } else if (isDelete) {
                jsonObject = convertRequestToJSON(tableName, columnSet, whereSetDelete, "delete");
            } else if (isUpdate) {
                jsonObject = convertRequestToJSON(tableName, columnSet, dataSet, whereSetUpdate, "update" );
            }
            logger.debug("final json = " + jsonObject);
            // Writing the view maintenance logs to a separate logfile
            writeJsonToFile(jsonObject);

//            File commitLogViewMaintenance = new File(System.getProperty("user.dir") + "/logs/viewMaintenceCommitLogsv2.log");
//            Charset charset = Charset.forName("US-ASCII");
//            try {
//                writer = new BufferedWriter(new FileWriter(commitLogViewMaintenance, true));
//                writer.write(jsonObject.toString() + "\n");
//                writer.flush();
//            } catch (IOException e) {
//                try {
//                    writer.close();
//                } catch (IOException e1) {
//                    e1.printStackTrace();
//                }
//                e.printStackTrace();
//            }
        }

        /**
         * Writes json to the commitLog file for view maintenance
         */
        private static boolean writeJsonToFile(JSONObject jsonObject)
        {
            // Writing the view maintenance logs to a separate logfile

            BufferedWriter writer = null;
            logger.debug("The system property for user.dir = {} ", System.getProperty("user.dir"));
            File commitLogViewMaintenance = new File(System.getProperty("user.dir") + "/logs/viewMaintenceCommitLogsv2.log");
            Charset charset = Charset.forName("US-ASCII");
            try {
                writer = new BufferedWriter(new FileWriter(commitLogViewMaintenance, true));
                writer.write(jsonObject.toString() + "\n");
                writer.flush();
            } catch (IOException e) {
                try {
                    writer.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                    return false;
                }
                e.printStackTrace();
                return false;
            }
            return  true;
        }

        /* Converts the update statement data to a json
         * Retains the where portion as is.
         */
        private static JSONObject convertRequestToJSON(String tableName, List<String> colList, List<Object> dataList, List<String> whereSetUpdate, String type) {
            JSONObject jsonObject = new JSONObject();
            
            jsonObject.put("operation_id", operation_id++);
            jsonObject.put("table", tableName);
            jsonObject.put("type", type);
            JSONObject dataObj = new JSONObject();
            dataObj.put(colList.get(0), dataList.get(0));
            jsonObject.put("data", dataObj);
            jsonObject.put("where", whereSetUpdate);
            String timestamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new java.util.Date());
            jsonObject.put("timestamp", timestamp);
            System.out.println(jsonObject);
            return jsonObject;
        }


        /*
        * Converts input request to JSON object
        *
        */

        private JSONObject convertRequestToJSON(String tableName, List<String> colList, List<Object> dataList, String type) {
            JSONObject jsonObject = new JSONObject();

            jsonObject.put("operation_id", operation_id++);
            jsonObject.put("table", tableName);
            jsonObject.put("type", type);
            int index = 0;
            if (type.equals("insert")) {
                JSONObject dataJSON = new JSONObject();
                if (colList.size() == dataList.size()) {
                    Iterator<String> iterCol = colList.iterator();
                    while (iterCol.hasNext()) {
                        String tempCol = iterCol.next();
                        dataJSON.put(tempCol, dataList.get(colList.indexOf(tempCol)));
                        index++;
                    }
                }
                jsonObject.put("data", dataJSON);
            } else if (type.equals("delete")) {
                Iterator<Object> iterCol = dataList.iterator();
                String tempCol ="";
                while (iterCol.hasNext()) {
                    tempCol = tempCol + " " + (String)iterCol.next();
                }
                jsonObject.put("where", tempCol);
            }

            String timestamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new java.util.Date());

            jsonObject.put("timestamp", timestamp);

            return jsonObject;
        }


        /*
        * Converts stack to JSON object
        *
        */

        private JSONObject convertStackToJSON(String colName, Stack rawStack) {
            JSONObject jsonObject = new JSONObject();
            int open = 0;
            int close = 0;
            List<String> tempStringList = new ArrayList<String>();
            while (!rawStack.isEmpty()) {
                Object o = rawStack.pop();
                if (o instanceof String) {
                    logger.debug(" whats inside = " + o.toString());
                    String tempStr = (String) o;
                    if (tempStr.equals("{")) {
                        open++;
                        if (open == close) {
                            logger.debug(" when open is equal to close " + tempStringList);
                            jsonObject.put(colName, tempStringList);
                        }
                    } else if (tempStr.equals("}")) {
                        close++;
                    } else {
                        if (open < close) {
                            logger.debug("adding " + tempStr);
                            tempStringList.add(tempStr);
                        }
                    }
                }


            }
            return jsonObject;
        }


        private void flush(FlushItem item) {
            EventLoop loop = item.ctx.channel().eventLoop();
            Flusher flusher = flusherLookup.get(loop);
            if (flusher == null) {
                Flusher alt = flusherLookup.putIfAbsent(loop, flusher = new Flusher(loop));
                if (alt != null)
                    flusher = alt;
            }

            flusher.queued.add(item);
            flusher.start();
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            if (ctx.channel().isOpen()) {
                UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), false);
                ChannelFuture future = ctx.writeAndFlush(ErrorMessage.fromException(cause, handler));
                // On protocol exception, close the channel as soon as the message have been sent
                if (cause instanceof ProtocolException) {
                    future.addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) {
                            ctx.close();
                        }
                    });
                }
            }
        }
    }

    /**
     * Include the channel info in the logged information for unexpected errors, and (if {@link #alwaysLogAtError} is
     * false then choose the log level based on the type of exception (some are clearly client issues and shouldn't be
     * logged at server ERROR level)
     */
    static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable> {
        private final Channel channel;
        private final boolean alwaysLogAtError;

        UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError) {
            this.channel = channel;
            this.alwaysLogAtError = alwaysLogAtError;
        }

        @Override
        public boolean apply(Throwable exception) {
            String message;
            try {
                message = "Unexpected exception during request; channel = " + channel;
            } catch (Exception ignore) {
                // We don't want to make things worse if String.valueOf() throws an exception
                message = "Unexpected exception during request; channel = <unprintable>";
            }

            if (!alwaysLogAtError && exception instanceof IOException) {
                if (ioExceptionsAtDebugLevel.contains(exception.getMessage())) {
                    // Likely unclean client disconnects
                    logger.debug(message, exception);
                } else {
                    // Generally unhandled IO exceptions are network issues, not actual ERRORS
                    logger.info(message, exception);
                }
            } else {
                // Anything else is probably a bug in server of client binary protocol handling
                logger.error(message, exception);
            }

            // We handled the exception.
            return true;
        }
    }
}
