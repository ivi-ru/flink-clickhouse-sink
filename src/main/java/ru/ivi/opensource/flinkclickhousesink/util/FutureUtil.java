package ru.ivi.opensource.flinkclickhousesink.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class FutureUtil {

    private FutureUtil() {

    }

    public static CompletableFuture<Void> allOf(List<CompletableFuture<Boolean>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}
