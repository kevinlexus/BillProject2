package com.dic.bill.dao;

import com.dic.bill.model.exs.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import javax.persistence.QueryHint;
import java.util.List;

public interface TaskDAO2 extends JpaRepository<Task, Integer> {

    /**
     * Вернуть список необработанных заданий, отсортированных по приоритету
     * по разрешенным к обмену УК
     */
    @Query("select t.id from Task t left join t.master d left join t.procUk uk left join uk.org o " +
            "where t.state in ('INS','ACK','RPT') and t.parent is null and t.id not in (:inWorkTaskId) " +
            "and (uk is null or o.isExchangeGis = true) " // либо пусто в УК, либо разрешен обмен с ГИС
            + "and (t.master is null or t.master.state in ('ACP'))")
    List<Integer> getAllUnprocessedNotActiveTaskIds(@Param("inWorkTaskId") List<Integer> inWorkTaskId);

    @Query("select distinct t from Task t join t.eolink e join e.debSubRequests d " +
            "where t.act.cd in ('GIS_IMP_DEB_SUB_RESPONSE', 'GIS_EXP_DEB_SUB_REQUEST') and t.state not in ('INS','ACK') and d.id in (:debRequestIds)")
    List<Task> findDistinctActiveTaskIdByDebRequestIds(@Param("debRequestIds") List<Integer> debRequestIds);

}
