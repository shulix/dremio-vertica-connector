/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.exec.store.jdbc.*;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.apache.log4j.Logger;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

@SourceType(value = "VERTICA", label = "Vertica", uiConfig = "vertica-layout.json", externalQuerySupported = true)
public class VerticaConf extends AbstractArpConf<VerticaConf> {
    private static final String ARP_FILENAME = "arp/implementation/vertica-arp.yaml";
    private static final ArpDialect ARP_DIALECT =
            AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
    private static final String DRIVER = "com.vertica.jdbc.Driver";

  @Tag(1)
  @DisplayMetadata(label = "Host")
  public String host;
  
    
  @Tag(2)
  @DisplayMetadata(label = "Port")
  public String port;
  
  @Tag(3)
  @DisplayMetadata(label = "Database")
  public String database;
  
  @Tag(4)
  @DisplayMetadata(label = "Username")
  public String user;


  @Tag(5)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;


  @Tag(6)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  
  public int fetchSize = 200;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String username = checkNotNull(this.user, "Missing username.");
    final String password = checkNotNull(this.password, "Missing password.");
    
    return String.format("jdbc:vertica://%s:%s/%s?"+"user="+ "%s"+"&password="+"%s",host, port, database, user, password);
    //return String.format("jdbc:vertica://192.168.0.23:5433/vmart?user=dbadmin&password=password");
  }



    @Override
    @VisibleForTesting
    public JdbcPluginConfig buildPluginConfig(
            JdbcPluginConfig.Builder configBuilder,
            CredentialsService credentialsService,
            OptionManager optionManager
    ){
        return configBuilder.withDialect(getDialect())
            .withFetchSize(fetchSize)
            .withDatasourceFactory(this::newDataSource)
            .clearHiddenSchemas()
            .build();
    }

    private CloseableDataSource newDataSource() {
        return DataSources.newGenericConnectionPoolDataSource(DRIVER,
            toJdbcConnectionString(), user, password, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, 10 , 300);
  }


    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }

    @VisibleForTesting
    public static ArpDialect getDialectSingleton() {
        return ARP_DIALECT;
    }
}
