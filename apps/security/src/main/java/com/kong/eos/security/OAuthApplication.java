package com.kong.eos.security;

import com.kong.eos.security.model.Account;
import com.kong.eos.security.model.AuthUser;
import com.kong.eos.security.repository.AccountRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.endpoint.CheckTokenEndpoint;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.SessionAttributes;

import java.security.Principal;
import java.util.HashMap;

@SpringBootApplication
@EnableAutoConfiguration
@SessionAttributes("authorizationRequest")
@ComponentScan("com.kong.eos.security")
@RestController
public class OAuthApplication {

    private Logger logger = LoggerFactory.getLogger(OAuthApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(OAuthApplication.class, args);
    }

    @Autowired
    AccountRepository accountRepository;

    @RequestMapping(value = "/user", method = RequestMethod.GET)
    public ResponseEntity getUser(OAuth2Authentication authentication) {
        Authentication oauth = authentication.getUserAuthentication();
        String userName = (String) oauth.getPrincipal();
        return new ResponseEntity<>(new HashMap<String, Object>() {{
            put("username", userName);
        }}, HttpStatus.OK);
    }

}
