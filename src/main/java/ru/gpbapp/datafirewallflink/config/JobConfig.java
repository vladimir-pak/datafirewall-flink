package ru.gpbapp.datafirewallflink.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Map;

public record JobConfig(
        String mqHost,
        int mqPort,
        String mqChannel,
        String mqQmgr,
        String mqInQueue,
        String mqOutQueue,
        String mqUser,
        String mqPassword
) {

    public static JobConfig fromArgs(ParameterTool pt) {
        ParameterTool params = pt == null
                ? ParameterTool.fromMap(Map.of())
                : pt;

        String host = params.get("mq.host", "localhost");
        int port = params.getInt("mq.port", 1414);
        String channel = params.get("mq.channel", "DEV.APP.SVRCONN");
        String qmgr = params.get("mq.qmgr", "QM1");
        String inQueue = params.get("mq.inQueue", "TEST.QUEUE");
        String outQueue = params.get("mq.outQueue", "REPLY.QUEUE");

        String defaultUser = "admin";
        String defaultPassword = "admin123";

        String envUser = System.getenv("MQ_USER");
        String envPassword = System.getenv("MQ_PASSWORD");

        if (envUser != null && !envUser.isBlank()) {
            defaultUser = envUser;
        }
        if (envPassword != null && !envPassword.isBlank()) {
            defaultPassword = envPassword;
        }

        String user = params.get("mq.user", defaultUser);
        String password = params.get("mq.password", defaultPassword);

        return new JobConfig(
                host,
                port,
                channel,
                qmgr,
                inQueue,
                outQueue,
                user,
                password
        );
    }
}