/**
 * @author laidingqing By CaiKong Network technology CO.,LTD
 * @create 2018-03-23 下午4:36
 * @desc
 **/
package com.kong.eos.security.exception;

public class AuthException extends RuntimeException{

    private static final long serialVersionUID = 1L;

    private Integer errCode;

    public AuthException(){}

    public AuthException(String message){
        super(message, null);
    }

    public AuthException(String message, Throwable cause){
        super(message, cause);
    }

    public AuthException(Integer errCode, String errorMessage, Throwable cause){
        super(errorMessage, cause);
        this.errCode = errCode;
    }


    public Integer getErrCode() {
        return errCode;
    }


    public void setErrCode(Integer errCode) {
        this.errCode = errCode;
    }
}
