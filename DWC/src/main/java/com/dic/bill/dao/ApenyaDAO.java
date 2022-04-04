package com.dic.bill.dao;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import com.dic.bill.model.scott.Ko;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.dic.bill.model.scott.Apenya;

/**
 * DAO сущности Apenya
 * @author Lev
 * @version 1.00
 *
 */
@Repository()
public interface ApenyaDAO extends JpaRepository<Apenya, Integer> {


	/**
	 * Получить все элементы по lsk
	 * @param lsk лиц.счет
	 * @param mg период
	 */
	@Query("select t from Apenya t "
			+ "where t.kart.lsk = :lsk and t.mg = :mg "
			+ "order by t.mg1")
	List<Apenya> getByLsk(@Param("lsk") String lsk, @Param("mg") String mg);

	/**
	 * Получить совокупную пеню
	 * @param lsk лиц.счет
	 * @param mg период
	 */
	@Query(value = "select sum(t.penya) from Apenya t "
			+ "where t.kart.lsk = :lsk and t.mg = :mg")
	BigDecimal getPenAmnt(@Param("lsk") String lsk, @Param("mg") String mg);

	/**
	 * Получить совокупную пеню по периоду задолженности
	 * @param lsk лиц.счет
	 * @param mg период
	 * @param mg1 период задолженности
	 */
	@Query(value = "select sum(t.penya) from Apenya t "
			+ "where t.kart.lsk = :lsk and t.mg = :mg and t.mg1 = :mg1")
	BigDecimal getPenAmntPeriod(@Param("lsk") String lsk, @Param("mg") String mg, @Param("mg1") String mg1);

}
