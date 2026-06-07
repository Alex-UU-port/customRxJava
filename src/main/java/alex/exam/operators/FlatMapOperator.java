package alex.exam.operators;

import alex.exam.Observable;
import alex.exam.Observer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class FlatMapOperator<T, R> extends Observable<R> {
    public FlatMapOperator(Observable<T> source, Function<T, Observable<R>> mapper) {
        super(observer -> {
            AtomicInteger pending = new AtomicInteger(1);

            source.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        Observable<R> inner = mapper.apply(item);
                        pending.incrementAndGet();
                        inner.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R r) { observer.onNext(r); }
                            @Override
                            public void onError(Throwable t) { observer.onError(t); }
                            @Override
                            public void onComplete() {
                                if (pending.decrementAndGet() == 0) {
                                    observer.onComplete();
                                }
                            }
                        });
                    } catch (Throwable t) {
                        observer.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onComplete() {
                    if (pending.decrementAndGet() == 0) {
                        observer.onComplete();
                    }
                }
            });
        });
    }
}
