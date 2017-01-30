package io.tenmax.cqlkit;

import com.datastax.driver.core.*;
import com.google.gson.*;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class RowUtils {
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public static String toString(
        DataType type,
        Object value)
    {
    	TypeCodec<Object> typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(type);
        if(typeCodec.getJavaType().getRawType().getName().equals(String.class.getName())) {
            return (String) value;
        } else if(typeCodec.getJavaType().getRawType().getName().equals(InetAddress.class.getName())) {
            return ((InetAddress) value).getHostAddress();
        } else if(type.getName() == DataType.Name.TIMESTAMP) {
            return toDateString((Date) value);
        } else {
            return typeCodec.format(value);
        }
    }



    public static JsonElement toJson(
            DataType type,
            Object value,
            boolean jsonColumn)
    {
        if(value == null) {
            return null;
        }
        TypeCodec<Object> typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(type);
        switch(type.getName()) {
            case BLOB:
                return new JsonPrimitive(typeCodec.format(value));
            case BOOLEAN:
                return new JsonPrimitive((Boolean)value);
            case BIGINT:
            case COUNTER:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case VARINT:
                return new JsonPrimitive((Number)value);
            case ASCII:
            case TEXT:
            case VARCHAR:
                if(jsonColumn) {
                    return new JsonParser().parse((String)value);
                } else {
                    return new JsonPrimitive((String) value);
                }
            case UUID:
            case INET:
            case TIMEUUID:
                return new JsonPrimitive(typeCodec.format(value));
            case TIMESTAMP:
                return new JsonPrimitive(toDateString((Date)value));
                //return new JsonPrimitive(((Date)value).getTime());
            case LIST:
            case SET:
                return collectionToJson(type, (Collection)value, jsonColumn);
            case MAP:
                return mapToJson(type, (Map)value, jsonColumn);
            case TUPLE:
            case UDT:
            case CUSTOM:
            default:
                throw new UnsupportedOperationException(
                        "The type is not supported now: " + type.getName());
        }
    }

    public static void setDateFormat(String pattern) {
        dateFormat = new SimpleDateFormat(pattern);
    }

    private static JsonElement mapToJson(
        DataType type,
        Map map,
        boolean jsonColumn)
    {
        DataType[] dataTypes = type.getTypeArguments().toArray(new DataType[]{});
        JsonObject root = new JsonObject();
        map.forEach((key, value) -> {
            if(value != null) {
                root.add(key.toString(), toJson(dataTypes[1], value, jsonColumn));
            }
        });
        return root;
    }

    private static JsonElement collectionToJson(
            DataType type,
            Collection collection,
            boolean jsonColumn)
    {
        DataType[] dataTypes = type.getTypeArguments().toArray(new DataType[]{});
        JsonArray array = new JsonArray();
        collection.forEach((value) -> {
            if(value != null) {
                array.add(toJson(dataTypes[0], value, jsonColumn));
            }
        });
        return array;
    }

    private static String toDateString(Date date) {
        /* protect against multithreaded access of static dateFormat */
        synchronized ( RowUtils.class ) {
            return date != null ? dateFormat.format(date) : null;
        }
    }
}
