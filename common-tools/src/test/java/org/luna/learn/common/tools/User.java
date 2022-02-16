package org.luna.learn.common.tools;

import org.luna.learn.common.annotation.Alias;
import org.luna.learn.common.annotation.Ignore;

public class User {

    @Alias("username")
    String name;
    int sex;
    int age;
    String address;
    Company company;
    @Ignore
    String note;

    public String toString() {
        return "{name=" + name + ",sex=" + sex + ",age=" + age
                + ",address=" + address
                + ",company=" + company
                + "}";
    }
}
