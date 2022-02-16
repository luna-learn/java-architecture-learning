package org.luna.learn.common.tools;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void mappingFields() {
        Map<String, Class<?>> maps = new HashMap<>();
        maps.put("name", String.class);
        maps.put("age", int.class);

        Map<String, Field> mapping = ReflectUtils
                .mappingField(User.class, maps, true);

        System.out.println(mapping);
    }

    @Test
    public void transform() {
        Map<String, Object> company = new HashMap<>();
        company.put("name", "alibaba");
        company.put("address", "hangzhou");

        Map<String, Object> userInfo = new HashMap<>();
        userInfo.put("username", "ZhangShan");
        userInfo.put("age", 23);
        userInfo.put("sex", 0);
        userInfo.put("address", "张三的家");
        userInfo.put("company", company);

        User user = ReflectUtils.fromMap(User.class, userInfo);
        System.out.println(user);

        System.out.println(ReflectUtils.toMap(user));

    }
}
