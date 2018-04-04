/**
 * @author laidingqing By CaiKong Network technology CO.,LTD
 * @create 2018-03-23 下午4:38
 * @desc
 **/
package com.kong.eos.security.repository;

import com.kong.eos.security.model.Account;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.io.Serializable;

public interface AccountRepository extends MongoRepository<Account, Serializable> {

    @Query("{userName: ?0}")
    public Account findByUserName(String userName);
}
