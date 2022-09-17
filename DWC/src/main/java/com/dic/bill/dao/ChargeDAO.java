package com.dic.bill.dao;

import com.dic.bill.dto.SumCharge;
import com.dic.bill.dto.SumFinanceFlow;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.model.scott.Charge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import javax.persistence.QueryHint;
import java.math.BigDecimal;
import java.util.List;


/**
 * DAO entity ChargeDAO
 * @author Lev
 * @version 1.01
 */
public interface ChargeDAO extends JpaRepository<Charge, Integer> {

	/**
	 * Получить записи текущих начислений по периодам
	 * @param lsk - лицевой счет
	 */
	@Query(value = "select coalesce(sum(t.summa),0) as summa "
			+ " from Charge t "
			+ "where t.kart.lsk=:lsk and t.type=1 ")
	@QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
	BigDecimal getChargeByPeriodAndLsk(@Param("lsk") String lsk);

	/**
	 * Получить сгруппированные записи начислений текущего периода
	 * @param lsk - лицевой счет
	 */
	@Query(value = "select t.usl.id as uslId, n.org.id as orgId, sum(t.summa) as summa "
			+ "from Charge t join t.kart k join k.nabor n on n.usl.id=t.usl.id "
			+ "where t.kart.lsk=:lsk and t.type=1 "
			+ "and coalesce(t.summa,0) <> 0 "
			+ "group by t.usl.id, n.org.id")
	List<SumUslOrgRec> getChargeByLskGrouped(@Param("lsk") String lsk);

	@Modifying
	@Query(value = "delete from Charge t where t.kart.lsk in (:lsk) and t.type in (:type)")
	void deleteAllByKartLskInAndTypeIn(@Param("lsk") List<String> lsks, @Param("type") List<Integer> types);


	@Query(value="select u.nm as name, t.test_opl as vol, t.test_cena as price, u.ed_izm as unit, t.summa\n" +
			"      from scott.kart k\n" +
			"      join scott.c_charge t on k.lsk=t.lsk and t.type=1\n" +
			"      join scott.usl u on t.usl=u.usl\n" +
			"      where k.k_lsk_id=?1\n" +
			"order by u.npp", nativeQuery = true)
	List<SumCharge> getChargeByKlsk(Long klskId);

}
