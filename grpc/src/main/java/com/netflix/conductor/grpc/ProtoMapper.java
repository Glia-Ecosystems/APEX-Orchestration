package com.netflix.conductor.grpc;

import com.google.protobuf.*;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.proto.WorkflowTaskPb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ProtoMapper implements conversion code between the internal models
 * used by Conductor (POJOs) and their corresponding equivalents in
 * the exposed Protocol Buffers interface.
 *
 * The vast majority of the mapping logic is implemented in the autogenerated
 * {@link AbstractProtoMapper} class. This class only implements the custom
 * logic for objects that need to be special cased in the API.
 */
public final class ProtoMapper extends AbstractProtoMapper {
    public static final ProtoMapper INSTANCE = new ProtoMapper();
    private static final int NO_RETRY_VALUE = -1;

    private ProtoMapper() {}

    /**
     * Convert an {@link Object} instance into its equivalent {@link Value}
     * ProtoBuf object.
     *
     * The {@link Value} ProtoBuf message is a variant type that can define any
     * value representable as a native JSON type. Consequently, this method expects
     * the given {@link Object} instance to be a Java object instance of JSON-native
     * value, namely: null, {@link Boolean}, {@link Double}, {@link String},
     * {@link Map}, {@link List}.
     *
     * Any other values will cause an exception to be thrown.
     * See {@link ProtoMapper#fromProto(Value)} for the reverse mapping.
     *
     * @param val a Java object that can be represented natively in JSON
     * @return an instance of a {@link Value} ProtoBuf message
     */
    @Override
    public Value toProto(Object val) {
        Value.Builder builder = Value.newBuilder();

        if (val == null) {
            builder.setNullValue(NullValue.NULL_VALUE);
        } else if (val instanceof Boolean) {
            builder.setBoolValue((Boolean) val);
        } else if (val instanceof Double) {
            builder.setNumberValue((Double) val);
        } else if (val instanceof String) {
            builder.setStringValue((String) val);
        } else if (val instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) val;
            Struct.Builder struct = Struct.newBuilder();
            for (Map.Entry<String, Object> pair : map.entrySet()) {
                struct.putFields(pair.getKey(), toProto(pair.getValue()));
            }
            builder.setStructValue(struct.build());
        } else if (val instanceof List) {
            ListValue.Builder list = ListValue.newBuilder();
            for (Object obj : (List<Object>)val) {
                list.addValues(toProto(obj));
            }
            builder.setListValue(list.build());
        } else {
            throw new ClassCastException("cannot map to Value type: "+val);
        }
        return builder.build();
    }

    /**
     * Convert a ProtoBuf {@link Value} message into its native Java object
     * equivalent.
     *
     * See {@link ProtoMapper#toProto(Object)} for the reverse mapping and the
     * possible values that can be returned from this method.
     *
     * @param any an instance of a ProtoBuf {@link Value} message
     * @return a native Java object representing the value
     */
    @Override
    public Object fromProto(Value any) {
        switch (any.getKindCase()) {
            case NULL_VALUE:
                return null;
            case BOOL_VALUE:
                return any.getBoolValue();
            case NUMBER_VALUE:
                return any.getNumberValue();
            case STRING_VALUE:
                return any.getStringValue();
            case STRUCT_VALUE:
                Struct struct = any.getStructValue();
                Map<String, Object> map = new HashMap<>();
                for (Map.Entry<String, Value> pair : struct.getFieldsMap().entrySet()) {
                    map.put(pair.getKey(), fromProto(pair.getValue()));
                }
                return map;
            case LIST_VALUE:
                List<Object> list = new ArrayList<>();
                for (Value val : any.getListValue().getValuesList()) {
                    list.add(fromProto(val));
                }
                return list;
            default:
                throw new ClassCastException("unset Value element: "+any);
        }
    }

    /**
     * Convert a WorkflowTaskList message wrapper into a {@link List} instance
     * with its contents.
     *
     * @param list an instance of a ProtoBuf message
     * @return a list with the contents of the message
     */
    @Override
    public List<WorkflowTask> fromProto(WorkflowTaskPb.WorkflowTask.WorkflowTaskList list) {
        return list.getTasksList().stream().map(this::fromProto).collect(Collectors.toList());
    }

    @Override public WorkflowTaskPb.WorkflowTask toProto(final WorkflowTask from) {
        final WorkflowTaskPb.WorkflowTask.Builder to = WorkflowTaskPb.WorkflowTask.newBuilder(super.toProto(from));
        if (from.getRetryCount() == null) {
            to.setRetryCount(NO_RETRY_VALUE);
        }
        return to.build();
    }

    @Override public WorkflowTask fromProto(final WorkflowTaskPb.WorkflowTask from) {
        final WorkflowTask workflowTask = super.fromProto(from);
        if (from.getRetryCount() == NO_RETRY_VALUE) {
            workflowTask.setRetryCount(null);
        }
        return workflowTask;
    }



    /**
     * Convert a list of {@link WorkflowTask} instances into a ProtoBuf wrapper object.
     *
     * @param list a list of {@link WorkflowTask} instances
     * @return a ProtoBuf message wrapping the contents of the list
     */
    @Override
    public WorkflowTaskPb.WorkflowTask.WorkflowTaskList toProto(List<WorkflowTask> list) {
        return WorkflowTaskPb.WorkflowTask.WorkflowTaskList.newBuilder()
                .addAllTasks(list.stream().map(this::toProto)::iterator)
                .build();
    }

    @Override
    public Any toProto(Any in) {
        return in;
    }

    @Override
    public Any fromProto(Any in) {
        return in;
    }
}
