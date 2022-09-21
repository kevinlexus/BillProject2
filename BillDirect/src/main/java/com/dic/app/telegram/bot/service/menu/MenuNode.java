package com.dic.app.telegram.bot.service.menu;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
public class MenuNode {
        private List<MenuNode> leaves = new LinkedList<MenuNode>();
        private MenuNode parent;
        private Menu menu;

        public MenuNode(Menu menu, MenuNode parent) {
            this.menu = menu;
            this.parent = parent;
        }
    }