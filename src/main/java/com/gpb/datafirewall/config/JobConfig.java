package com.gpb.datafirewall.config;

import org.apache.flink.api.java.utils.ParameterTool;

import com.gpb.datafirewall.vault.dto.VaultSecretsDto;

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

    public static JobConfig fromArgs(ParameterTool pt, VaultSecretsDto vaultSecrets) {
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

        String user = vaultSecrets.mqUser();
        String password = vaultSecrets.mqPassword();

        if ("mq".equalsIgnoreCase(backend)) {
            if (user == null || user.isBlank()) {
                throw new IllegalArgumentException("mqUser must be provided in vault");
            }

            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("mqPassword must be provided in vault");
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