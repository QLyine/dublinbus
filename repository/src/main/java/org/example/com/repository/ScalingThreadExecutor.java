package org.example.com.repository;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

public class ScalingThreadExecutor extends ThreadPoolExecutor {

  public ScalingThreadExecutor(
      final int corePoolSize,
      final int maximumPoolSize,
      final long keepAliveTime,
      final TimeUnit keepAliveUnit
  ) {
    super(
        corePoolSize,
        maximumPoolSize,
        keepAliveTime,
        keepAliveUnit,
        new DynamicBlockingQueue<>(new LinkedTransferQueue<>()),
        new ForceQueuePolicy()
    );
  }

  public ScalingThreadExecutor(
      final int corePoolSize,
      final int maximumPoolSize,
      final long keepAliveTime,
      final TimeUnit keepAliveUnit,
      final TransferQueue<Runnable> workQueue
  ) {
    super(
        corePoolSize,
        maximumPoolSize,
        keepAliveTime,
        keepAliveUnit,
        new DynamicBlockingQueue<>(workQueue),
        new ForceQueuePolicy()
    );
  }

  @Override
  public void setRejectedExecutionHandler(final RejectedExecutionHandler handler) {
    throw new IllegalArgumentException("Cant set rejection handler");
  }

  private static class ForceQueuePolicy implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
      try {
        //Rejected work add to Queue.
        executor.getQueue().put(r);
      } catch (final InterruptedException e) {
        //should never happen since we never wait
        throw new RejectedExecutionException(e);
      }
    }
  }

  private static class DynamicBlockingQueue<E> implements TransferQueue<E> {

    private final TransferQueue<E> delegate;

    public DynamicBlockingQueue(final TransferQueue<E> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean offer(final E o) {
      return tryTransfer(o);
    }

    @Override
    public boolean add(final E o) {

      if (delegate.add(o)) {
        return true;
      } else {// Not possible in our case
        throw new IllegalStateException("Queue full");
      }
    }

    @Override
    public E remove() {
      return delegate.remove();
    }

    @Override
    public E poll() {
      return delegate.poll();
    }

    @Override
    public E element() {
      return delegate.element();
    }

    @Override
    public E peek() {
      return delegate.peek();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
      return delegate.iterator();
    }

    @Override
    public Object[] toArray() {
      return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
      return delegate.toArray(a);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
      return delegate.containsAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
      return delegate.addAll(c);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
      return delegate.removeAll(c);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
      return delegate.retainAll(c);
    }

    @Override
    public void clear() {
      delegate.clear();
    }

    @Override
    public void put(final E e) throws InterruptedException {
      delegate.put(e);
    }

    @Override
    public boolean offer(final E e, final long timeout, final TimeUnit unit)
        throws InterruptedException {
      return delegate.offer(e, timeout, unit);
    }

    @Override
    public E take() throws InterruptedException {
      return delegate.take();
    }

    @Override
    public E poll(final long timeout, final TimeUnit unit) throws InterruptedException {
      return delegate.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
      return delegate.remainingCapacity();
    }

    @Override
    public boolean remove(final Object o) {
      return delegate.remove(o);
    }

    @Override
    public boolean contains(final Object o) {
      return delegate.contains(o);
    }

    @Override
    public int drainTo(final Collection<? super E> c) {
      return delegate.drainTo(c);
    }

    @Override
    public int drainTo(final Collection<? super E> c, final int maxElements) {
      return delegate.drainTo(c, maxElements);
    }

    @Override
    public boolean tryTransfer(final E e) {
      return delegate.tryTransfer(e);
    }

    @Override
    public void transfer(final E e) throws InterruptedException {
      delegate.transfer(e);
    }

    @Override
    public boolean tryTransfer(final E e, final long timeout, final TimeUnit unit)
        throws InterruptedException {
      return delegate.tryTransfer(e, timeout, unit);
    }

    @Override
    public boolean hasWaitingConsumer() {
      return delegate.hasWaitingConsumer();
    }

    @Override
    public int getWaitingConsumerCount() {
      return delegate.getWaitingConsumerCount();
    }
  }

}
