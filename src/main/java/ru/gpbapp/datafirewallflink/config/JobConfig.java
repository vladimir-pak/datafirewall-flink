package ru.gpbapp.datafirewallflink.config;

import org.apache.flink.api.java.utils.ParameterTool;

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
        if (pt == null) {
            pt = ParameterTool.fromMap(java.util.Map.of());
        }

        String host = pt.get("mq.host", "localhost");
        int port = pt.getInt("mq.port", 1414);
        String channel = pt.get("mq.channel", "DEV.APP.SVRCONN");
        String qmgr = pt.get("mq.qmgr", "QM1");
        String inQueue = pt.get("mq.inQueue", "TEST.QUEUE");
        String outQueue = pt.get("mq.outQueue", "REPLY.QUEUE");

        String user = "admin";
        String password = "admin123";

        String envUser = System.getenv("MQ_USER");
        String envPassword = System.getenv("MQ_PASSWORD");

        if (envUser != null && !envUser.isBlank()) {
            user = envUser;
        }
        if (envPassword != null && !envPassword.isBlank()) {
            password = envPassword;
        }

        user = pt.get("mq.user", user);
        password = pt.get("mq.password", password);

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