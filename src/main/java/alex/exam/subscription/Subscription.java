package alex.exam.subscription;

import alex.exam.Disposable;
import java.util.concurrent.atomic.AtomicBoolean;

public class Subscription implements Disposable {
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    @Override
    public void dispose() {
        disposed.set(true);
        //System.out.println("   [Subscription] dispose() вызван, disposed = " + disposed.get());
    }

    @Override
    public boolean isDisposed() {
        boolean result = disposed.get();
        //System.out.println("   [Subscription] isDisposed() = " + result);
        return result;
    }
}