/**
 * @author laidingqing By CaiKong Network technology CO.,LTD
 * @create 2018-03-26 上午9:37
 * @desc
 **/
package com.kong.eos.security;

import com.kong.eos.security.model.Account;
import com.kong.eos.security.model.Authority;
import com.kong.eos.security.repository.AccountRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.Date;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AccountRepositoryTest {

    @Autowired
    private AccountRepository accountRepository;

    private PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    @Test
    public void testCreateAccount(){
        Account account = new Account();
        account.setClientId("kong_cloud");
        account.setUserName("laidingqing");
        account.setEncryptPassword(passwordEncoder.encode("123456"));
        account.setCreatedAt(new Date());
        account.setAuthority(Collections.singletonList(Authority.USER));

        accountRepository.save(account);
    }
}
