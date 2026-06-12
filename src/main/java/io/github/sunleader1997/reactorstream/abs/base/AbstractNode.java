package io.github.sunleader1997.reactorstream.abs.base;

public abstract class AbstractNode<T> {
    public abstract void destroy() throws Exception;

    /**
     * push item to the queue of next node
     * @param item data item
     */
    public void push(T item){
        // TODO push item to the queue of next node
    }
}
