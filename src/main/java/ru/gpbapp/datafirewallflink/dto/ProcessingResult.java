package ru.gpbapp.datafirewallflink.dto;

import ru.gpbapp.datafirewallflink.mq.MqReply;

public class ProcessingResult {

    private MqReply shortReply;
    private String detailJson;

    public ProcessingResult() {
    }

    public ProcessingResult(MqReply shortReply, String detailJson) {
        this.shortReply = shortReply;
        this.detailJson = detailJson;
    }

    public MqReply getShortReply() {
        return shortReply;
    }

    public void setShortReply(MqReply shortReply) {
        this.shortReply = shortReply;
    }

    public String getDetailJson() {
        return detailJson;
    }

    public void setDetailJson(String detailJson) {
        this.detailJson = detailJson;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "shortReply=" + shortReply +
                ", detailJson='" + detailJson + '\'' +
                '}';
    }
}
