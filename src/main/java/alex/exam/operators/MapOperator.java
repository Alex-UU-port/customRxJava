package alex.exam.operators;

import alex.exam.Observable;
import alex.exam.Observer;
import alex.exam.Disposable;

import java.util.function.Function;

public class MapOperator<T, R> extends Observable<R> {

    public MapOperator(Observable<T> source, Function<? super T, ? extends R> mapper) {
        super((observer, disposable) -> {
            source.subscribe(new Observer<T>() {
                private boolean hasError = false;  // Флаг, что ошибка уже была

                @Override
                public void onNext(T item) {
                    if (hasError || disposable.isDisposed()) {
                        return;
                    }
                    try {
                        observer.onNext(mapper.apply(item));
                    } catch (Throwable t) {
                        hasError = true;
                        observer.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (!hasError && !disposable.isDisposed()) {
                        hasError = true;
                        observer.onError(t);
                    }
                }

                @Override
                public void onComplete() {
                    if (!hasError && !disposable.isDisposed()) {
                        observer.onComplete();
                    }
                }
            });
        });
    }
}