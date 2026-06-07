package alex.exam.operators;

import alex.exam.Observable;
import alex.exam.Observer;

import java.util.function.Predicate;

public class FilterOperator<T> extends Observable<T> {
    public FilterOperator(Observable<T> source, Predicate<T> predicate) {
        super(observer -> source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                try {
                    if (predicate.test(item)) {
                        observer.onNext(item);
                    }
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
