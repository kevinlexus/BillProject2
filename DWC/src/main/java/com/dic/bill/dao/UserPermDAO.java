package com.dic.bill.dao;

import com.dic.bill.model.scott.UserPerm;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface UserPermDAO extends JpaRepository<UserPerm, Integer> {

    List<UserPerm> findByUkReuAndTpCd(String ukReu, String tpCd);

}
