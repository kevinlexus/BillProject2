package com.dic.bill.impl;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;

import com.dic.bill.UlistTpDAO;



@Repository
public class UlistTpDAOImpl implements UlistTpDAO {

	@PersistenceContext
    private EntityManager em;

	//конструктор
    //public UlistTpDAOImpl() {

    //}

}
