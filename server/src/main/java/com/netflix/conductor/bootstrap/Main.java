/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.bootstrap;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearch;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.grpc.server.GRPCServer;
import com.netflix.conductor.grpc.server.GRPCServerProvider;
import com.netflix.conductor.jetty.server.JettyServerProvider;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.util.Optional;

/**
 * @author Viren Entry point for the server
 */
public class Main {

    public static void main(String[] args) throws Exception {

        BootstrapUtil.loadConfigFile(args.length > 0 ? args[0] : System.getenv("CONDUCTOR_CONFIG_FILE"));

        if (args.length == 2) {
            System.out.println("Using log4j config " + args[1]);
            PropertyConfigurator.configure(new FileInputStream(new File(args[1])));
        }

        final Injector bootstrapInjector = Guice.createInjector(new BootstrapModule());
        final ModulesProvider modulesProvider = bootstrapInjector.getInstance(ModulesProvider.class);
        final Injector serverInjector = Guice.createInjector(modulesProvider.get());

        final Optional<EmbeddedElasticSearch> embeddedElasticSearch = serverInjector.getInstance(EmbeddedElasticSearchProvider.class).get();
        embeddedElasticSearch.ifPresent(BootstrapUtil::startEmbeddedElasticsearchServer);

        BootstrapUtil.setupIndex(serverInjector.getInstance(IndexDAO.class));

        try {
            serverInjector.getInstance(IndexDAO.class).setup();
        } catch (final Exception e) {
            e.printStackTrace(System.err);
            System.exit(3);
        }


        System.out.println("\n\n\n");
        System.out.println("                     _            _             ");
        System.out.println("  ___ ___  _ __   __| |_   _  ___| |_ ___  _ __ ");
        System.out.println(" / __/ _ \\| '_ \\ / _` | | | |/ __| __/ _ \\| '__|");
        System.out.println("| (_| (_) | | | | (_| | |_| | (__| || (_) | |   ");
        System.out.println(" \\___\\___/|_| |_|\\__,_|\\__,_|\\___|\\__\\___/|_|   ");
        System.out.println("\n\n\n");

        final Optional<GRPCServer> grpcServer = serverInjector.getInstance(GRPCServerProvider.class).get();

        grpcServer.ifPresent(BootstrapUtil::startGRPCServer);

        serverInjector.getInstance(JettyServerProvider.class).get().ifPresent(server -> {
            try {
                server.start();
            } catch (final Exception ioe) {
                ioe.printStackTrace(System.err);
                System.exit(3);
            }
        });
    }

}
