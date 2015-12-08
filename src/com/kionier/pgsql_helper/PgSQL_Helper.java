/* Npgsql helper library for Java Copyright (C) 2015 Kionier
* For guides on how to use this library, please go to http://kionier.com/npgsql-helper/.
* This software is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License (GPL) or GNU Lesser General Public License (LGPL) as published by the Free Software Foundation, either version 2 or later.
* This software is distributed WITHOUT ANY WARRANTY; see the GNU General Public License for more details. http://www.gnu.org/licenses/.
*/
package com.kionier.pgsql_helper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import org.postgresql.util.PGobject;

import com.google.gson.Gson;
import java.sql.ResultSetMetaData;

public class PgSQL_Helper {

    private String _conn = null;
    private final Gson _gson = new Gson();

    public PgSQL_Helper(String conn) {
        _conn = conn;
    }

    public PGobject PGjson(Object o) throws SQLException {
        PGobject pgo = new PGobject();
        pgo.setType("json");
        pgo.setValue(this._gson.toJson(o));
        return pgo;
    }

    public class Scalar implements AutoCloseable {

        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        public CallableStatement cs = null;
        String function = null;
        int n;
        
        public Scalar(String statement) throws SQLException {
            this(statement, -1);
        }

        Scalar(String function, int n) throws SQLException {
            this.n = n;
            this.con = DriverManager.getConnection(_conn);
            // this.con.setAutoCommit(false);
            if (n > 0) {
                char[] p = new char[n];
                Arrays.fill(p, '?');
                // p[0] = "?";
                this.cs = this.con.prepareCall(String.format("{call %1$s%2$s}", function, Arrays.toString(p).replace("[", "(").replace("]", ")")));
            } else if (n == 0) {
                this.cs = this.con.prepareCall(String.format("{call %1$s()}", function));
            }  else {
                this.cs = this.con.prepareCall(function);
            }
            // this.cs = this.con.prepareCall("{call }");
        }

        @Override
        public void close() {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (st != null) {
                    st.close();
                    st = null;
                }
                if (cs != null) {
                    cs.close();
                    cs = null;
                }
                if (con != null) {
                    con.close();
                    con = null;
                }

            } catch (SQLException ex) {
            }
        }

        public Object Exec() throws SQLException {
            this.rs = cs.executeQuery();
            if (this.rs.next()) {

                return this.rs.getObject(1);
            }
            return null;
        }
    }

    public class Query implements AutoCloseable {

        Connection con = null;
        Statement st = null;
        ResultSet[] rs = null;
        public CallableStatement cs = null;
        String function = null;
        int n;

        public Query(String statement) throws SQLException {
            this(statement, -1);
        }

        public Query(String function, int n) throws SQLException {
            this.n = n;
            this.con = DriverManager.getConnection(_conn);
            this.con.setAutoCommit(false);

            if (n > 0) {
                char[] p = new char[n];
                Arrays.fill(p, '?');
                // p[0] = "?";
                this.cs = this.con.prepareCall(String.format("{call %1$s%2$s}", function, Arrays.toString(p).replace("[", "(").replace("]", ")")));
            } else if (n == 0) {
                this.cs = this.con.prepareCall(String.format("{call %1$s()}", function));
            } else {
                this.cs = this.con.prepareCall(function);
            }
        }

        @Override
        public void close() {
            try {
                if (rs != null) {
                    for (ResultSet rss : rs) {
                        rss.close();
                        rss = null;
                    }
                    rs = null;
                }
                if (st != null) {
                    st.close();
                    st = null;
                }
                if (cs != null) {
                    cs.close();
                    cs = null;
                }
                if (con != null) {
                    con.close();
                    con = null;
                }

            } catch (SQLException ex) {
            }
        }

        public ResultSet[] Exec() throws SQLException {
            ResultSet rs = cs.executeQuery();
            ResultSetMetaData metadata = rs.getMetaData();
            if (this.n >= 0 && rs.getCursorName() == null&& metadata.getColumnCount() == 1 && metadata.getColumnType(1) == 1111) {
                ArrayList<ResultSet> rss = new ArrayList<ResultSet>();
                while (rs.next()) {
                    rss.add((ResultSet) rs.getObject(1));
                }
                this.rs = rss.toArray(new ResultSet[rss.size()]);

            } else {
                this.rs = new ResultSet[]{rs};
            }
            con.commit();
            return this.rs;
        }
    }

    public class NonQuery implements AutoCloseable {

        Connection con = null;
        Statement st = null;
        public CallableStatement cs = null;
        ResultSet rs = null;
        String function = null;
        int n;
        
        public NonQuery(String statement) throws SQLException {
            this(statement, -1);
        }

        NonQuery(String function, int n) throws SQLException {
            this.n = n;
            this.con = DriverManager.getConnection(_conn);
            // this.con.setAutoCommit(false);
            if (n > 0) {
                char[] p = new char[n];
                Arrays.fill(p, '?');
                // p[0] = "?";
                this.cs = this.con.prepareCall(String.format("{call %1$s%2$s}", function, Arrays.toString(p).replace("[", "(").replace("]", ")")));
            } else if (n == 0) {
                this.cs = this.con.prepareCall(String.format("{call %1$s()}", function));
            } else {
                this.cs = this.con.prepareCall(function);
            }
        }

        public void close() {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (st != null) {
                    st.close();
                    st = null;
                }
                if (cs != null) {
                    cs.close();
                    cs = null;
                }
                if (con != null) {
                    con.close();
                    con = null;
                }

            } catch (SQLException ex) {
            }
        }

        public void Exec() throws SQLException {
            cs.execute();
            // return cs.execute();
        }
    }

}
