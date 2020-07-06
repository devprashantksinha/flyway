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

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.database.base.Type;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VerticaSchema extends Schema<VerticaDatabase,VerticaTable> {
    /**
     * Creates a new schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public VerticaSchema(JdbcTemplate jdbcTemplate, VerticaDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM v_catalog.schemata WHERE schema_name=?", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        int objectCount = jdbcTemplate.queryForInt(
                "SELECT count(*) FROM v_catalog.all_tables WHERE schema_name=? and table_type = 'TABLE'",
                name);
        return objectCount == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name) + " CASCADE");
    }

    @Override
    protected void doClean() throws SQLException {
        for (String statement : generateDropStatementsForViews()) {
            jdbcTemplate.execute(statement);
        }

        for (Table table : allTables()) {
            table.drop();
        }

        for (String statement : generateDropStatementsForSequences()) {
            jdbcTemplate.execute(statement);
        }

        // Vertica does not support base types, enums or domains

        // Includes scalar functions and aggregate functions:
        for (String statement : generateDropStatementsForFunctions()) {
            jdbcTemplate.execute(statement);
        }

        for (Type type : allTypes()) {
            type.drop();
        }
    }

    @Override
    protected VerticaTable[] doAllTables() throws SQLException {
        List<String> tableNames =
                jdbcTemplate.queryForStringList(
                        //Search for all the table names
                        "SELECT t.table_name FROM v_catalog.all_tables t" +
                                //in this schema
                                " WHERE schema_name=?" +
                                //Querying for 'TABLE' in Vertica will exclude views, system tables and temporary tables
                                " and table_type =  'TABLE'",
                        name);
        //Vertica will drop projections, when using cascade, but it will not drop views.

        VerticaTable[] tables = new VerticaTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new VerticaTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return null;
    }

    private List<String> generateDropStatementsForViews() throws SQLException {
        List<String> viewNames =
                jdbcTemplate.queryForStringList(
                        //Search for all the table names
                        "SELECT t.table_name FROM v_catalog.all_tables t" +
                                //in this schema
                                " WHERE schema_name=?" +
                                //Querying for 'VIEW' in Vertica will exclude user-defined tables, system tables and temporary tables
                                " and table_type = 'VIEW'",
                        name);

        List<String> statements = new ArrayList<String>();
        for (String viewName : viewNames) {
            statements.add("DROP VIEW IF EXISTS " + database.quote(name, viewName));
        }
        return statements;
    }

    private List<String> generateDropStatementsForFunctions() throws SQLException {
        List<Map<String, String>> rows =
                jdbcTemplate.queryForList(
                        "select * from user_functions where schema_name = ? and procedure_type = 'User Defined Function'",
                        name);

        List<String> statements = new ArrayList<String>();
        for (Map<String, String> row : rows) {
            statements.add("DROP FUNCTION IF EXISTS " + database.quote(name, row.get("function_name")) + "(" + row.get("function_argument_type") + ")");
        }
        return statements;
    }

    private List<String> generateDropStatementsForSequences() throws SQLException {
        List<String> sequenceNames =
                jdbcTemplate.queryForStringList(
                        "SELECT sequence_name FROM v_catalog.sequences WHERE sequence_schema=?", name);

        List<String> statements = new ArrayList<String>();
        for (String sequenceName : sequenceNames) {
            statements.add("DROP SEQUENCE IF EXISTS " + database.quote(name, sequenceName));
        }

        return statements;
    }
}
