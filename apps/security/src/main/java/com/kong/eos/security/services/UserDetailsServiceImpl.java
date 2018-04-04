/**
 * @author laidingqing By CaiKong Network technology CO.,LTD
 * @create 2018-03-23 下午4:18
 * @desc
 **/
package com.kong.eos.security.services;

import com.kong.eos.security.model.Account;
import com.kong.eos.security.model.AuthUser;
import com.kong.eos.security.model.Authority;
import com.kong.eos.security.repository.AccountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;

@Service
public class UserDetailsServiceImpl implements UserDetailsService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private AccountRepository accountRepository;

    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        Account account =  accountRepository.findByUserName(userName);
        if (account == null) {
            throw new UsernameNotFoundException(String.format("用户[%s]不存在!", userName));
        }
        Collection<GrantedAuthority> grantedAuthority = new ArrayList<>();

        for(Authority authority : account.getAuthority()){
            grantedAuthority.add( new SimpleGrantedAuthority(String.valueOf(authority.getValue())));
        }
        return new AuthUser(account.getId(), userName, account.getEncryptPassword(), grantedAuthority);
    }
}
