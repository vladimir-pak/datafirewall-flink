package com.gpb.datafirewall.services;

public final class HandlerRoutingState {

    private volatile DynamicHandler currentHandler;

    public HandlerRoutingState(DynamicHandler defaultHandler) {
        this.currentHandler = defaultHandler == null ? DynamicHandler.FLINK : defaultHandler;
    }

    public DynamicHandler currentHandler() {
        return currentHandler;
    }

    public void update(DynamicHandler handler) {
        if (handler != null) {
            this.currentHandler = handler;
        }
    }
}
