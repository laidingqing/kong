/**
 * @author laidingqing By CaiKong Network technology CO.,LTD
 * @create 2018-03-23 下午4:39
 * @desc
 **/
package com.kong.eos.security.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

@org.springframework.data.mongodb.core.mapping.Document(collection = "accounts")
public class Account implements Serializable {

    @Id
    private String id;
    /**
     * 指定应用ID
     */
    @JsonIgnore
    private String clientId;
    /**
     * 登录用户名（手机号码）
     */
    @Indexed
    private String userName;
    /**
     * 角色
     */
    private List<Authority> authority;

    /**
     * 加密后密码
     */
    @Transient
    @JsonIgnore
    private String password;
    /**
     * 加密后密码
     */
    @JsonIgnore
    private String encryptPassword;
    /**
     * 创建时间
     */
    private Date createdAt;

    public Account() { }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public List<Authority> getAuthority() {
        return authority;
    }

    public void setAuthority(List<Authority> authority) {
        this.authority = authority;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEncryptPassword() {
        return encryptPassword;
    }

    public void setEncryptPassword(String encryptPassword) {
        this.encryptPassword = encryptPassword;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
}
