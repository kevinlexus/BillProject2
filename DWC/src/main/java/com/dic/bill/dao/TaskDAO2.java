package com.dic.bill.dao;

import com.dic.bill.model.exs.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface TaskDAO2 extends JpaRepository<Task, Integer> {

    /**
     * Вернуть список необработанных заданий, отсортированных по приоритету
     * по разрешенным к обмену УК
     */
    @Query("select t from Task t left join t.master d left join t.procUk uk left join uk.org o " +
            "where t.state in ('INS','ACK','RPT') and t.parent is null and t.id not in (:inWorkTaskId) " +
            "and (uk = null or o.isExchangeGis = true) " // либо пусто в УК, либо разрешен обмен с ГИС
            + "and (t.master is null or t.master.state in ('ACP')) order by nvl(t.priority,0) desc, t.id")
    List<Task> getAllUnprocessedAndNotActive(@Param("inWorkTaskId") List<Integer> inWorkTaskId);

    /**
     * Вернуть список необработанных заданий, отсортированных по приоритету
     * по разрешенным к обмену УК
     */
    @Query("select t from Task t left join t.master d left join t.procUk uk left join uk.org o " +
            "where t.state in ('INS','ACK','RPT') and t.parent is null " +
            "and (uk = null or o.isExchangeGis = true) " // либо пусто в УК, либо разрешен обмен с ГИС
            + "and (t.master is null or t.master.state in ('ACP')) order by nvl(t.priority,0) desc, t.id")
    List<Task> getAllUnprocessed();

}
