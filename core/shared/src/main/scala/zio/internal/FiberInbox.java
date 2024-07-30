package zio.internal;


import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

class FiberInbox {

    static final int BUFFER_SIZE = 128;

    static class Node {
        // TODO: Test with and without padding
        final AtomicInteger deqidx = new AtomicInteger(0);
        final AtomicReferenceArray<FiberMessage> items = new AtomicReferenceArray<>(BUFFER_SIZE);
        final AtomicInteger enqidx = new AtomicInteger(0);
        volatile Node next = null;

        Node (final FiberMessage item) {
            items.lazySet(0, item);
        }


        boolean casNext(Node val) {
            return nextOffset.compareAndSet(this, null, val);
        }

        private static final VarHandle nextOffset;

        static {
            try {
                nextOffset = MethodHandles.lookup().findVarHandle(Node.class, "next", FiberInbox.Node.class);
            } catch (Exception e) {
                throw new Error(e);
            }
        }
    }

    private volatile Node head;
    private volatile Node tail;

    private final FiberMessage taken = FiberMessage.unit();

    public FiberInbox() {
        final Node startSentinel = new Node(null);
        head = startSentinel;
        tail = startSentinel;
    }



    /**
     * Progress Condition: Lock-Free
     *
     * @param item must not be null
     */
    void enqueue(FiberMessage item) {
        if (item == null) throw new NullPointerException();
        while (true) {
            final Node ltail = tail;
            if (ltail.items.get(BUFFER_SIZE-1) != null) { // This node is full
                if (ltail != tail) continue;
                final Node lnext = ltail.next;
                if (lnext == null) {
                    final Node newNode = new Node(item);
                    if (ltail.casNext(newNode)) {
                        casTail(ltail, newNode);
                        return;
                    }
                } else {
                    casTail(ltail, lnext);
                }
                continue;
            }
            for (int i=ltail.enqidx.get(); i < BUFFER_SIZE; i++) {
                if (ltail.items.get(i) != null) continue;
                if (ltail.items.compareAndSet(i, null, item)) {
                    ltail.enqidx.lazySet(i+1);
                    return;
                }
                if (ltail != tail) break;
            }
        }
    }

    /**
     * Progress condition: lock-free
     */
    FiberMessage dequeue() {
        Node lhead = head;
        Node node = lhead;
        while (node != null) {
            if (node.items.get(0) == null) return null;   // This node is empty
            if (node.items.get(BUFFER_SIZE-1) == taken) { // This node has been drained, check if there is another one
                node = node.next;
                continue;
            }
            for (int i=node.deqidx.get(); i < BUFFER_SIZE; i++) {
                final FiberMessage item = node.items.get(i);
                if (item == null) return null;            // This node is empty
                if (item == taken) continue;
                if (node.items.compareAndSet(i, item, taken)) {
                    node.deqidx.lazySet(i+1);
                    if (node != lhead && head == lhead) casHead(lhead, node);
                    //lhead.next = lhead;                   // Do self-linking to help the GC
                    return item;
                }
                if (lhead != head) break;
            }
        }
        return null;                                      // Queue is empty
    }

    private void casTail(Node cmp, Node val) {
        tailOffset.compareAndSet(this, cmp, val);
    }

    private void casHead(Node cmp, Node val) {
        headOffset.compareAndSet(this, cmp, val);
    }

    public boolean isEmpty() {
        // TODO: Can we make this O(1)? I assumed head == null would work but does not
        Node node = head;
        while (node != null) {
            if (node.items.get(0) == null) return true;
            if (node.items.get(BUFFER_SIZE-1) == taken) { // This node has been drained, check if there is another one
                node = node.next;
                continue;
            }
            for (int i=node.deqidx.get(); i < BUFFER_SIZE; i++) {
                final FiberMessage item = node.items.get(i);
                if (item == null) return true;            // This node is empty
                if (item == taken) continue;
                return false;
            }
        }
        return true;
    }

    private static final VarHandle tailOffset;
    private static final VarHandle headOffset;
    static {
        try {
            tailOffset = MethodHandles.lookup().findVarHandle(FiberInbox.class, "tail", FiberInbox.Node.class);
            headOffset = MethodHandles.lookup().findVarHandle(FiberInbox.class, "head", FiberInbox.Node.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}