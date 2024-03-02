package com.dic.bill.dao;

import com.dic.bill.model.exs.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface TaskDAO2 extends JpaRepository<Task, Integer> {

    /**
     * Вернуть список необработанных заданий, отсортированных по приоритету
     * по разрешенным к обмену УК
     * Запрос вида:
     * select task0_.id as col_0_0_
     * from exs.task task0_
     *          left outer join exs.task task1_ on task0_.dep_id = task1_.id
     *          left outer join exs.eolink eolink2_ on task0_.fk_proc_uk = eolink2_.id
     *          left outer join scott.t_org org3_ on eolink2_.reu = org3_.reu
     * where (task0_.state in ('INS', 'ACK', 'RPT'))
     *   and (task0_.parent_id is null)
     *   and (task0_.id not in (?)) -- список id активных тасков
     *   and (eolink2_.id is null or org3_.is_exchange_gis = 1)
     *   and (task0_.dep_id is null or task1_.state in ('ACP'));
     */
    @Query("select t.id from Task t left join t.master d left join t.procUk uk left join uk.org o " +
            "where t.state in ('INS','ACK','RPT') and t.parent is null and t.id not in (:inWorkTaskId) " +
            "and (uk is null or o.isExchangeGis = true) " // либо пусто в УК, либо разрешен обмен с ГИС
            + "and (t.master is null or t.master.state in ('ACP'))")
    List<Integer> getAllUnprocessedNotActiveTaskIds(@Param("inWorkTaskId") List<Integer> inWorkTaskId);
    @Query("select distinct t from Task t join t.eolink e join e.debSubRequests d " +
            "where t.act.cd in ('GIS_IMP_DEB_SUB_RESPONSE', 'GIS_EXP_DEB_SUB_REQUEST') and t.state not in ('INS','ACK') and d.id in (:debRequestIds)")
    List<Task> findDistinctActiveTaskIdByDebRequestIds(@Param("debRequestIds") List<Integer> debRequestIds);

    Optional<Task> findByCd(String cd);

}
