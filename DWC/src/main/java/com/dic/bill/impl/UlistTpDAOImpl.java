package com.dic.bill.impl;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.stereotype.Repository;


@Repository
public class UlistTpDAOImpl {

	@PersistenceContext
    private EntityManager em;

}
