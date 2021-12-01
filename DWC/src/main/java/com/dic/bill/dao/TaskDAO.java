package com.dic.bill.dao;

import com.dic.bill.model.exs.Task;

import java.util.List;


public interface TaskDAO {

    List<Task> getByTp(String tp);

    List<Task> getByTaskAddrTp(Task task, String addrTp, String addrTpx, Integer appTp);

    Task getByTguid(Task task, String tguid);

    Boolean getChildAnyErr(Task task);

    Task getByCd(String cd); // новый метод 2
}
