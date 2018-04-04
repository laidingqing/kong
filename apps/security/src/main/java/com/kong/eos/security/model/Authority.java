package com.kong.eos.security.model;

import com.kong.eos.security.exception.AuthException;

public enum Authority {

    USER(0, "用户"),
    ADMIN(1, "管理员");

    private int value;
    private String name;

    Authority(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return this.value;
    }

    public String getName() {
        return this.name;
    }

    public static Authority get(int code) {
        switch (code) {
            case 0:
                return USER;
            case 1:
                return ADMIN;
            default:
                throw new AuthException(String.format("无法转换账户类型: %s", String.valueOf(code)));
        }
    }
}
