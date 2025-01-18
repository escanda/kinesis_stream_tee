package eu.escandasys.kinesis;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ClosingIterator<U> implements Iterator<Optional<U>>, Closeable {
    public static <U> ClosingIterator<U> of(Supplier<Boolean> fetchCond, Supplier<Optional<U>> supplier, Runnable closer) {
        return new ClosingIterator<>(fetchCond, supplier, closer);
    }

    private final Supplier<Boolean> fetchCond;
    private final Supplier<Optional<U>> supplier;
    private final Runnable closer;
    
    public ClosingIterator(Supplier<Boolean> fetchCond, Supplier<Optional<U>> supplier, Runnable closer) {
        this.fetchCond = fetchCond;
        this.supplier = supplier;
        this.closer = closer;
    }

    @Override
    public void close() throws IOException {
        closer.run();
    }

    @Override
    public boolean hasNext() {
        return fetchCond.get();
    }

    @Override
    public Optional<U> next() {
        return supplier.get();
    }
}
