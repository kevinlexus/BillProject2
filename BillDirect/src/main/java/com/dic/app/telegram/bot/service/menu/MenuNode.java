package com.dic.app.telegram.bot.service.menu;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
public class MenuNode implements Serializable {
    private List<MenuNode> leaves = new LinkedList<>();
    private MenuNode parent;
    private Menu menu;

    private String callBackData;

    public MenuNode(Menu menu, MenuNode parent) {
        this.menu = menu;
        this.parent = parent;
    }
}