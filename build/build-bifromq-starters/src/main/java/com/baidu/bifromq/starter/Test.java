package com.baidu.bifromq.starter;

public class Test<K, V> {
    class Node<K, V> {
        K key;
        V value;
        Node<K, V> parent;
        Node<K, V> left;
        Node<K, V> right;
    }

    private void rotateLeft(Node<K, V> x) {
        if (x != null) {
            Node<K, V> node = x.right;
            x.right = node.left;
            if (node.left != null) {
                node.left.parent = x;
            }
            node.parent = x.parent;
            if (x.parent == null) {

            } else if (x.parent.left == x) {
                x.parent.left = node;
            } else {
                x.parent.right = node;
            }
            x.parent = node;
            node.left = x;
        }
    }

    private void rotateRight(Node<K, V> x) {
        if (x != null) {
            Node<K, V> node = x.left;
            x.left = node.right;
            if (node.right != null) {
                node.right.parent = x;
            }
            node.parent = x.parent;
            if (x.parent == null) {

            } else if (x.parent.left == x) {
                x.parent.left = node;
            } else {
                x.parent.right = node;
            }
            x.parent = node;
            node.left = x;
        }
    }
}
