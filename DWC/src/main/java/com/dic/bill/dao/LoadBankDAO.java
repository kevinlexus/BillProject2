package com.dic.bill.dao;

import com.dic.bill.model.scott.LoadBank;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface LoadBankDAO extends JpaRepository<LoadBank, Integer> {

    @Modifying
    @Query(
            value = "truncate table scott.load_bank",
            nativeQuery = true
    )
    void truncate();

}
