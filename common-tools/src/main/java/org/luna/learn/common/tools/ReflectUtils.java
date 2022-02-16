package org.luna.learn.common.tools;

import org.luna.learn.common.annotation.Alias;
import org.luna.learn.common.annotation.Ignore;
import org.luna.learn.common.types.Tuple;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ReflectUtils {

    private static Class<?>[] BASE_TYPES = {
            Number.class,
            Boolean.class,
            Byte.class,
            Double.class,
            Float.class,
            Integer.class,
            Long.class,
            Short.class,
            String.class
    };

    private static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }

    private static <T> boolean isEmpty(T[] arr) {
        return arr == null || arr.length == 0;
    }

    private static boolean isBaseType(Class<?> cls) {
        if (cls.isPrimitive()) {
            return true;
        } else {
            for (Class<?> c: BASE_TYPES) {
                if (c == cls) {
                    return true;
                }
            }
            return false;
        }
    }

    private static Field findField(Class<?> cls,
                                   String name,
                                   Class<?> type,
                                   boolean ignoreCase) {
        if (cls == null || isEmpty(name)) {
            return null;
        }
        String a = name;
        if (ignoreCase) {
            a = a.toLowerCase();
        }
        for (Field field: cls.getDeclaredFields()) {
            Ignore ignore = field.getDeclaredAnnotation(Ignore.class);
            if (ignore != null
                    || (type != null && !field.getType().equals(type))) {
                continue;
            }
            Alias alias = field.getDeclaredAnnotation(Alias.class);
            String b = alias != null ? alias.value() : field.getName();
            if (ignoreCase ? b.toLowerCase().equals(a) : b.equals(a)) {
                return field;
            }
        }
        return null;
    }

    private static Method findGetMethod(Class<?> cls,
                                        String name,
                                        Class<?> type,
                                        final boolean ignoreCase) {
        if (cls == null|| isEmpty(name)) {
            return null;
        }
        String a = name;
        if (ignoreCase) {
            a = a.toLowerCase();
        }
        for (Method method: cls.getMethods()) {
            Ignore ignore = method.getDeclaredAnnotation(Ignore.class);
            if (ignore != null
                    || method.getName().startsWith("get")
                    || (type != null && !method.getReturnType().equals(type))
                    || method.getParameterTypes().length != 0) {
                continue;
            }
            Alias alias = method.getDeclaredAnnotation(Alias.class);
            String b = alias != null ? alias.value() : method.getName();
            if (ignoreCase ? b.toLowerCase().equals(a) : b.equals(a)) {
                return method;
            }
        }
        return null;
    }

    private static Method findSetMethod(Class<?> cls,
                                        String name,
                                        Class<?> type,
                                        final boolean ignoreCase) {
        if (cls == null || isEmpty(name)) {
            return null;
        }
        String a = name;
        if (ignoreCase) {
            a = a.toLowerCase();
        }
        for (Method method: cls.getMethods()) {
            Ignore ignore = method.getDeclaredAnnotation(Ignore.class);
            if (ignore != null
                    || method.getName().startsWith("set")
                    || method.getParameterTypes().length != 1
                    || (type != null && type.equals(method.getParameterTypes()[0]))) {
                continue;
            }
            Alias alias = method.getDeclaredAnnotation(Alias.class);
            String b = alias != null ? alias.value() : method.getName();
            if (ignoreCase ? b.toLowerCase().equals(a) : b.equals(a)) {
                return method;
            }
        }
        return null;
    }

    public static Map<String, Field> mappingField(Class<?> cls,
                                                  Map<String, Class<?>> maps,
                                                  final boolean ignoreCase) {
        return maps.entrySet().stream()
                .map(e -> Tuple.of(e.getKey(), findField(cls, e.getKey(), e.getValue(), ignoreCase)))
                .filter(e -> e._2 != null)
                .collect(Collectors.toMap(e -> e._1, e -> e._2, (a, b) -> a));
    }

    public static Map<Integer, Field> mappingFieldOrdered(Class<?> cls,
                                                  Map<Integer, Tuple<String, Class<?>>> maps,
                                                  final boolean ignoreCase) {
        return maps.entrySet().stream()
                .map(e -> Tuple.of(e.getKey(), findField(cls, e.getValue()._1, e.getValue()._2, ignoreCase)))
                .filter(e -> e._2 != null)
                .collect(Collectors.toMap(e -> e._1, e -> e._2, (a, b) -> a));
    }

    public static Map<String, Method> mappingGetMethod(
            Class<?> cls,
            Map<String, Class<?>> maps,
            final boolean ignoreCase) {
        return maps.entrySet().stream()
                .map(e -> Tuple.of(e.getKey(), findGetMethod(cls, e.getKey(), e.getValue(), ignoreCase)))
                .filter(e -> e._2 != null)
                .collect(Collectors.toMap(e -> e._1, e -> e._2, (a, b) -> a));
    }

    public static Map<Integer, Method> mappingGetMethodOrdered (
            Class<?> cls,
            Map<Integer, Tuple<String, Class<?>>> maps,
            final boolean ignoreCase) {
        return maps.entrySet().stream()
                .map(e -> Tuple.of(e.getKey(), findGetMethod(cls, e.getValue()._1, e.getValue()._2, ignoreCase)))
                .filter(e -> e._2 != null)
                .collect(Collectors.toMap(e -> e._1, e -> e._2, (a, b) -> a));
    }

    public static Map<String, Method> mappingSetMethod(
            Class<?> cls,
            Map<String, Class<?>> maps,
            final boolean ignoreCase) {
        return maps.entrySet().stream()
                .map(e -> Tuple.of(e.getKey(), findSetMethod(cls, e.getKey(), e.getValue(), ignoreCase)))
                .filter(e -> e._2 != null)
                .collect(Collectors.toMap(e -> e._1, e -> e._2, (a, b) -> a));
    }

    public static Map<Integer, Method> mappingSetMethodOrdered(
            Class<?> cls,
            Map<Integer, Tuple<String, Class<?>>> maps,
            final boolean ignoreCase) {
        return maps.entrySet().stream()
                .map(e -> Tuple.of(e.getKey(), findSetMethod(cls, e.getValue()._1, e.getValue()._2, ignoreCase)))
                .filter(e -> e._2 != null)
                .collect(Collectors.toMap(e -> e._1, e -> e._2, (a, b) -> a));
    }

    public static void fromMap(Object o, Map<String, Object> map) {
        if (o == null || map == null || map.size() == 0) {
            return;
        }
        try {
            Class<?> cls = o.getClass();
            Map<String, Integer> records = new HashMap<>();
            for (Method method: cls.getMethods()) {
                Ignore ignore = method.getDeclaredAnnotation(Ignore.class);
                if (ignore != null
                        || !method.getName().startsWith("set")
                        || method.getParameterTypes().length != 1) {
                    continue;
                }
                Alias alias = method.getDeclaredAnnotation(Alias.class);
                String name = alias != null ? alias.value() : method.getName().substring(3);
                if (map.containsKey(name)) {
                    records.put(name, 1);
                    Object value = map.get(name);
                    if (value != null
                            && !isBaseType(method.getParameterTypes()[0])
                            && value instanceof Map) {

                        Object p = method.getParameterTypes()[0].newInstance();
                        fromMap(p, (Map) value);
                        method.invoke(o, p);
                    } else {
                        method.invoke(o, value);
                    }
                }
            }
            for (Field field: cls.getDeclaredFields()) {
                Ignore ignore = field.getDeclaredAnnotation(Ignore.class);
                if (ignore != null) {
                    continue;
                }
                Alias alias = field.getDeclaredAnnotation(Alias.class);
                String name = alias != null ? alias.value() : field.getName();
                if (!records.containsKey(name) || map.containsKey(name)) {
                    field.setAccessible(true);
                    Object value = map.get(name);
                    if (value != null
                            && !isBaseType(field.getType())
                            && value instanceof Map) {
                        Object p = field.getType().newInstance();
                        fromMap(p, (Map) value);
                        field.set(o, p);
                    } else {
                        field.set(o, value);
                    }
                }
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 将 Map 映射到类并返回实例对象
     * @param cls 类
     * @param map Map
     * @param <T>
     * @return
     */
    public static <T> T fromMap(Class<? extends T> cls, Map<String, Object> map) {
        if (map == null || map.size() == 0) {
            return null;
        }
        try {
            T instance = cls.newInstance();
            fromMap(instance, map);
            return instance;
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 将对象实例转换为 Map
     * @param o
     * @param <T>
     * @return
     */
    public static <T> Map<String, Object> toMap(T o) {
        if (o == null) {
            return null;
        }
        try {
            Map<String, Object> result = new HashMap<>();
            Class<?> cls = o.getClass();
            for (Method method: cls.getDeclaredMethods()) {
                Ignore ignore = method.getDeclaredAnnotation(Ignore.class);
                if (ignore != null
                        || !method.getName().startsWith("get")
                        || method.getParameterTypes().length != 0
                        || method.getReturnType().equals(void.class)) {
                    continue;
                }
                Alias alias = method.getDeclaredAnnotation(Alias.class);
                String name = alias != null ? alias.value() : method.getName().substring(3);
                Object value = method.invoke(o);
                if (value != null
                        && !isBaseType(method.getReturnType())) {
                    result.put(name, toMap(value));
                } else {
                    result.put(name, value);
                }
            }
            for (Field field: cls.getDeclaredFields()) {
                Ignore ignore = field.getDeclaredAnnotation(Ignore.class);
                if (ignore != null) {
                    continue;
                }
                Alias alias = field.getDeclaredAnnotation(Alias.class);
                String name = alias != null ? alias.value() : field.getName();
                field.setAccessible(true);
                Object value = field.get(o);
                if (value != null
                        && !isBaseType(field.getType())) {
                    result.put(name, toMap(value));
                } else {
                    result.put(name, value);
                }
            }
            return result;
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
