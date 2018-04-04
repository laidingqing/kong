/**
 * @author laidingqing By CaiKong Network technology CO.,LTD
 * @create 2018-03-23 下午5:26
 * @desc
 **/
package com.kong.eos.security.services;

import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.util.StringUtils;

public class ExtAuthenticationProvider  extends DaoAuthenticationProvider {

    public ExtAuthenticationProvider(UserDetailsService userDetailsService) {
        this.setUserDetailsService(userDetailsService);
        this.setPasswordEncoder(new BCryptPasswordEncoder());
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String username = authentication.getPrincipal().toString();
        String password = authentication.getCredentials().toString();
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
            throw new BadCredentialsException("用户名或密码必须输入");
        } else  {
            Object details = authentication.getDetails();
            return super.authenticate(authentication);
        }
    }
}
