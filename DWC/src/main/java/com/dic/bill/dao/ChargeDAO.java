package com.dic.bill.dao;

import com.dic.bill.dto.SumRec;
import com.dic.bill.dto.SumUslOrgRec;
import com.dic.bill.model.scott.Charge;
import org.springframework.data.jpa.repository.JpaRepository;
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
}
