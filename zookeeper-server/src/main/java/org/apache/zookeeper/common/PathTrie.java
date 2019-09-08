 /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 字典树，又称为单词查找树
 * PathTrie
 *   字典树完成配额目录的增删查
 *   将拥有对应配额属性的节点设置标记属性property
 *   在zk中的目录结构为/zookeeper/quota/xxx(可以有多级目录)/zookeeper_limits
 *
 *
 * a class that implements prefix matching for 
 * components of a filesystem path. the trie
 * looks like a tree with edges mapping to 
 * the component of a path.
 * 为文件系统路径的*组件实现前缀匹配的类。 trie 看起来像一棵树，边缘映射到路径的组件。
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       / 
 *      (bc)
 *   cf/
 *   (cf)
 *
 *
 *
 *   zk中的目录 /zookeeper/quota/a/b/c/zookeeper_limits 对应的PathTrie结构为
 *
 *                   root
 *                  /
 *                a
 *               /
 *             b
 *           /
 *         c
 *       /
 * zookeeper_limits
 *
 */    
public class PathTrie {
    /**
     * the logger for this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);
    
    /**
     * the root node of PathTrie
     * PathTrie的根节点
     */
    private final TrieNode rootNode ;

    // TrieNode是字典树中每个node的情况，主要记录parent，以及children的set
    static class TrieNode {
        //属性，看源码显示,就是设置了配额的节点
        boolean property = false;
        //记录子节点相对路径 与 TrieNode的mapping
        final Map<String, TrieNode> children;
        // 父节点
        TrieNode parent = null;
        /**
         * create a trienode with parent
         * as parameter
         * @param parent the parent of this trienode
         */
        //构造时，设置parent
        private TrieNode(TrieNode parent) {
            children = new HashMap<String, TrieNode>();
            // 父节点
            this.parent = parent;
        }
        
        /**
         * get the parent of this node
         * @return the parent node
         */
        TrieNode getParent() {
            return this.parent;
        }
        
        /**
         * set the parent of this node
         * @param parent the parent to set to
         */
        void setParent(TrieNode parent) {
            this.parent = parent;
        }
        
        /**
         * a property that is set 
         * for a node - making it 
         * special.
         */
        void setProperty(boolean prop) {
            this.property = prop;
        }
        
        /** the property of this
         * node 
         * @return the property for this
         * node
         */
        boolean getProperty() {
            return this.property;
        }
        /**
         *
         * 添加childName的相对路径到map,注意:在调用方设置node的parent
         *
         * add a child to the existing node
         * @param childName the string name of the child
         * @param node the node that is the child
         */
        void addChild(String childName, TrieNode node) {
            synchronized(children) {
                if (children.containsKey(childName)) {
                    return;
                }
                children.put(childName, node);
            }
        }
     
        /**
         * delete child from this node
         * @param childName the string name of the child to 
         * be deleted
         */
        void deleteChild(String childName) {
            synchronized(children) {
                if (!children.containsKey(childName)) {
                    return;
                }
                // 获取子节点
                TrieNode childNode = children.get(childName);
                // this is the only child node.
                // 如果子节点的子节点个数为1
                if (childNode.getChildren().length == 1) {  //如果这个儿子只有1个儿子,那么就把这个儿子丢掉
                    childNode.setParent(null);//被删除的子节点,parent设置为空
                    children.remove(childName);
                }
                else {
                    // their are more child nodes
                    // so just reset property.
                    childNode.setProperty(false);//否则这个儿子还有其他儿子，标记它不是没有配额限制,这里有个bug，就是数量为0时，也进入这个逻辑
                }
            }
        }
        
        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        //根据childName从map中取出对应的TrieNode
        TrieNode getChild(String childName) {
            synchronized(children) {
               if (!children.containsKey(childName)) {
                   return null;
               }
               else {
                   return children.get(childName);
               }
            }
        }

        /**
         * get the list of children of this 
         * trienode.
         * @return the string list of its children
         */
        //获取子节点String[]列表
        String[] getChildren() {
           synchronized(children) {
               return children.keySet().toArray(new String[0]);
           }
        }
        
        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized(children) {
                for (String str: children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }
    }
    
    /**
     * construct a new PathTrie with
     * a root node of /
     * 创建根节点
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null);
    }
    
    /**
     * 字典树的增
     *
     * add a path to the path trie 
     * @param path
     */
    public void addPath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");//将路径按照"/" split开
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {//从1开始因为路径都是"/"开头的,pathComponents[0]会是""
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                parent.addChild(part, new TrieNode(parent));//一方面parent将这个child记录在map，一方面将这个child node进行初始化以及设置parent
            }
            parent = parent.getChild(part);//进入到对应的child
        }
        parent.setProperty(true);//最后这个节点设置配额属性
    }
    
    /**
     * 字典树的删
     *
     * TODO: deletePath传入的参数,并不会一定精确到叶子节点,也就是可能会到某个目录，再调用realParent.deleteChild进行TrieNode的deleteChild操作
     *
     * delete a path from the trie
     * @param path the path to be deleted
     */
    public void deletePath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) { 
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                //the path does not exist 
                return;
            }
            parent = parent.getChild(part);
            LOG.info("{}",parent);
        }
        TrieNode realParent  = parent.getParent();//得到被删除TrieNode的parent
        realParent.deleteChild(part);//将这个node从parent的子列表中删除
    }
    
    /**
     * 字典树的查
     * 1.将path按/分开，进行字典树查找，从根开始
     * 2.记住路径中最后一个拥有配额标记的TrieNode
     *
     * return the largest prefix for the input path.
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    public String findMaxPrefix(String path) {//找到最近的一个拥有配额标记的祖先节点
        if (path == null) {
            return null;
        }
        if ("/".equals(path)) {
            return path;
        }
        String[] pathComponents = path.split("/");//按照/分开
        TrieNode parent = rootNode;
        List<String> components = new ArrayList<String>();
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        int i = 1;
        String part = null;
        StringBuilder sb = new StringBuilder();
        int lastindex = -1;
        while((i < pathComponents.length)) {
            if (parent.getChild(pathComponents[i]) != null) {
                part = pathComponents[i];
                parent = parent.getChild(part);//一层层到子节点
                components.add(part);
                if (parent.getProperty()) {//如果对应的子节点有标记
                    lastindex = i-1;//更新最后一个有标记的节点(也就是最近的有标记的祖先)
                }
            }
            else {
                break;
            }
            i++;
        }
        for (int j=0; j< (lastindex+1); j++) {
            sb.append("/" + components.get(j));
        }
        return sb.toString();//返回最近的,有标记的祖先的路径
    }

    /**
     * clear all nodes
     */
    public void clear() {
        for(String child : rootNode.getChildren()) {
            rootNode.deleteChild(child);
        }
    }
}
