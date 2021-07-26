package com.dic.bill.dao;

import com.dic.bill.model.scott.Ko;

import java.util.List;


public interface KoDAO {

    Ko getByKlsk(long klsk);

    List<Ko> getKoByAddrTpFlt(Integer addrTp, String flt);

}
