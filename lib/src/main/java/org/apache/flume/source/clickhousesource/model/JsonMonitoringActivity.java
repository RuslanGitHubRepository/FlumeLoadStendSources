package org.apache.flume.source.clickhousesource.model;

import com.google.common.reflect.Reflection;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.flume.FlumeException;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class JsonMonitoringActivity {
    private static final String regex = "([a-z])([A-Z]+)";
    private static final String replacement = "$1_$2";
    private final JsonObject jsonObject;

    public JsonMonitoringActivity(MonitoringActivity monitoringActivity) {
        this.jsonObject = activityToJson(monitoringActivity);
    }

    public JsonObject toJSONObject() {
        return jsonObject;
    }

    private JsonObject activityToJson(MonitoringActivity activity) throws FlumeException {
        final String prefix = "get";
        JsonObject jsonObject = new JsonObject();
        Method[] getMethods = Arrays.stream(MonitoringActivity.class.getMethods())
                .filter(o -> o.getName().startsWith(prefix))
                .peek(o -> o.setAccessible(Boolean.TRUE))
                .toArray(Method[]::new);
        for (Method method : getMethods) {
            String fieldName = method.getName()
                    .substring(prefix.length())
                    .replaceAll(regex, replacement)
                    .toLowerCase();
            try {
                Object invoke = method.invoke(activity);
                Class<?> clazz = invoke.getClass();
                if (Integer.class.equals(clazz)) {
                    jsonObject.add(fieldName, new JsonPrimitive((Integer) invoke));
                } else if (Byte.class.equals(clazz)) {
                    jsonObject.add(fieldName, new JsonPrimitive((Byte) invoke));
                } else if (Long.class.equals(clazz)) {
                    jsonObject.add(fieldName, new JsonPrimitive((Long) invoke));
                } else if (String.class.equals(clazz)) {
                    jsonObject.add(fieldName, new JsonPrimitive((String) invoke));
                } else if (Short.class.equals(clazz)) {
                    jsonObject.add(fieldName, new JsonPrimitive((Short) invoke));
                } else if (LocalDateTime.class.equals(clazz)) {
                    jsonObject.add(fieldName, new JsonPrimitive(((LocalDateTime) invoke).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
                } else if (fieldName.equals("ui_hierarchy_name") || fieldName.equals("parameters_name") || fieldName.equals("parameters_value")) {
                    jsonObject.add(fieldName, stringCollectionToJson((List<String>) invoke));
                } else if (fieldName.equals("ui_hierarchy_ctrl")) {
                    jsonObject.add(fieldName, integerCollectionToJson((List<Integer>) invoke));
                }
            } catch (Exception ex) {
                throw new FlumeException(ex);
            }
        }
        return jsonObject;
    }

    private JsonArray stringCollectionToJson(List<String> strings) {
        JsonArray jsonArray = new JsonArray();
        strings.stream()
                .map(JsonPrimitive::new)
                .forEach(jsonArray::add);
        return jsonArray;
    }

    private JsonArray integerCollectionToJson(List<Integer> numbers) {
        JsonArray jsonArray = new JsonArray();
        numbers.stream()
                .map(JsonPrimitive::new)
                .forEach(jsonArray::add);
        return jsonArray;
    }

}
