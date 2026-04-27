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

        String backend = params.get("messaging.backend", "mq");

        String host = params.get("mq.host", "localhost");
        int port = params.getInt("mq.port", 1414);
        String channel = params.get("mq.channel", "DEV.APP.SVRCONN");
        String qmgr = params.get("mq.qmgr", "QM1");
        String inQueue = params.get("mq.inQueue", "IN.Q");
        String outQueue = params.get("mq.outQueue", "OUT.Q");

        String user = params.get("mq.user", null);
        String password = params.get("mq.password", null);

        String envUser = System.getenv("MQ_USER");
        String envPassword = System.getenv("MQ_PASSWORD");

        if (envUser != null && !envUser.isBlank()) {
            user = envUser;
        }
        if (envPassword != null && !envPassword.isBlank()) {
            password = envPassword;
        }

        if ("mq".equalsIgnoreCase(backend)) {
            if (user == null || user.isBlank()) {
                throw new IllegalArgumentException("mq.user must be provided (param or ENV MQ_USER)");
            }

            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("mq.password must be provided (param or ENV MQ_PASSWORD)");
            }
        }

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