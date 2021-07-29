package com.dic.bill.dao;

import com.dic.bill.model.sec.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface UserDAO extends JpaRepository<User, Integer> {

    @Query("select t from User t where t.cd = :cd")
    User getByCd(@Param("cd") String cd);

}
