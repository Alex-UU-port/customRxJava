package alex.exam.operators;

import alex.exam.Observable;
import alex.exam.Observer;

import java.util.function.Function;

public class MapOperator<T, R> extends Observable<R> {
    public MapOperator(Observable<T> source, Function<T, R> mapper) {
        super(observer -> source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                try {
                    observer.onNext(mapper.apply(item));
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
                observer.onComplete();
            }
        }));
    }
}