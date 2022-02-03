package com.dic.bill.dao;

import com.dic.bill.model.exs.DebSubRequest;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface DebSubRequestDAO extends JpaRepository<DebSubRequest, Integer> {

    List<DebSubRequest> getAllByHouseGuid(String guid);

    Optional<DebSubRequest> getByRequestGuid(String guid);

}
