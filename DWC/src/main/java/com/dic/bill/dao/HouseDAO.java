package com.dic.bill.dao;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.dic.bill.model.scott.House;
import org.springframework.data.jpa.repository.QueryHints;

import javax.persistence.QueryHint;


public interface HouseDAO extends JpaRepository<House, Integer> {

	@Query(value = "select t from com.dic.bill.model.scott.House t where nvl(t.psch,0) = 0 ")
	List<House> getNotClosed();

	@QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
	Optional<House> findByGuid(String guid);

	List<House> findByGuidIsNotNull();

	@Query(value = "select distinct t.kul||t.nd as kulNd from scott.c_houses t", nativeQuery = true)
	List<String> getAllKulNds();
}
