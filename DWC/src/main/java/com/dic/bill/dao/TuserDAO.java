package com.dic.bill.dao;

import com.dic.bill.model.scott.Tuser;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface TuserDAO extends JpaRepository<Tuser, Integer> {

    Tuser findByCd(String cd);

    Optional<Tuser> getByGuid(@Param("guid") String guid);

}
