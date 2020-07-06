/*
 * Copyright 2010-2020 Redgate Software Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.vertica;

import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.RowMapper;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

public class VerticaConnection extends Connection<VerticaDatabase> {
    private final String originalRole;

    protected VerticaConnection(VerticaDatabase database, java.sql.Connection connection) {
        super(database, connection);
        try {
            originalRole = jdbcTemplate.queryForString("SELECT CURRENT_USER()");
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to determine current user", e);
        }
    }

    @Override
    protected void doRestoreOriginalState() throws SQLException {
        // Reset the role to its original value in case a migration or callback changed it
        jdbcTemplate.execute("SET ROLE " + originalRole );
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.query("SHOW search_path", new RowMapper<String>() {
            @Override
            public String mapRow(ResultSet rs) throws SQLException {
                // All "show" commands in Vertica return two columns: "name" and "setting",
                // but we just want the value in the second column:
                //
                //     name     |                      setting
                // -------------+---------------------------------------------------
                // search_path | "$user", public, v_catalog, v_monitor, v_internal
                return rs.getString("setting");
            }
        }).get(0);
    }

    @Override
    public Schema getSchema(String name) {
        return new VerticaSchema(jdbcTemplate, database, name);
    }

    @Override
    public void changeCurrentSchemaTo(Schema schema) {
        try {
            if (schema.getName().equals(originalSchemaNameOrSearchPath) || originalSchemaNameOrSearchPath.startsWith(schema.getName() + ",") || !schema.exists()) {
                return;
            }

            if (StringUtils.hasText(originalSchemaNameOrSearchPath)) {
                doChangeCurrentSchemaOrSearchPathTo(schema.toString() + "," + originalSchemaNameOrSearchPath);
            } else {
                doChangeCurrentSchemaOrSearchPathTo(schema.toString());
            }
        } catch (SQLException e) {
            throw new FlywaySqlException("Error setting current schema to " + schema, e);
        }
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        if (!StringUtils.hasLength(schema)) {
            jdbcTemplate.execute("SET SEARCH_PATH = v_catalog");
            return;
        }
        jdbcTemplate.execute("SET SEARCH_PATH = " + schema);
    }
}
