package com.dic.bill.dao;

import com.dic.bill.model.exs.DebRequest;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface DebRequestDAO extends JpaRepository<DebRequest, Integer> {

    List<DebRequest> getAllByHouseGuid(String guid);

    Optional<DebRequest> getByRequestGuid(String guid);

}
