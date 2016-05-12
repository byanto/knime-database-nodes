/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Nov 20, 2015 (budiyanto): created
 */
package org.knime.base.node.io.database.tablecreator.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;

/**
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBUtil {
    static final String SQLITE = "sqlite";

    static final String POSTGRESQL = "postgresql";

    static final String MYSQL = "mysql";

    // Default SQL Types
    /** Default SQL-type for Strings. */
    static final String SQL_TYPE_STRING = "varchar(255)";

    /** Default SQL-type for Booleans. */
    static final String SQL_TYPE_BOOLEAN = "boolean";

    /** Default SQL-type for Integers. */
    static final String SQL_TYPE_INTEGER = "integer";

    /** Default SQL-type for Doubles. */
    static final String SQL_TYPE_DOUBLE = "numeric(30,10)";

    /** Default SQL-type for Timestamps. */
    static final String SQL_TYPE_DATEANDTIME = "timestamp";

    /** Default SQL-type for Date. */
    static final String SQL_TYPE_BLOB = "blob";

    static String[] getDatabaseTypes(final String db) {
        switch (db.toLowerCase()) {
            case SQLITE:
                return getSQLiteTypes();
            case POSTGRESQL:
                return getPostgreSQLTypes();
            case MYSQL:
                return getMySQLTypes();
            default:
                break;
        }
        return null;
    }

    static private String[] getSQLiteTypes() {
        return new String[]{"DOUBLE SQLITE", "VARCHAR SQLITE", "INTEGER SQLITE"};
    }

    static private String[] getPostgreSQLTypes() {
        return new String[]{"DOUBLE POSTGRESQL", "VARCHAR POSTGRESQL", "INTEGER POSTGRESQL"};
    }

    static private String[] getMySQLTypes() {
        return new String[]{"DOUBLE MYSQL", "VARCHAR MYSQL", "INTEGER MYSQL"};
    }

    static Map<DataType, Set<String>> getSqlTypesMap() {
        Map<DataType, Set<String>> map = new LinkedHashMap<DataType, Set<String>>();
        // BooleanCell
        Set<String> compatibleTypes = new TreeSet<String>();
        compatibleTypes.add(SQL_TYPE_BOOLEAN);
        map.put(BooleanCell.TYPE, compatibleTypes);

        // IntCell
        compatibleTypes = new TreeSet<String>();
        compatibleTypes.add(SQL_TYPE_INTEGER);
        map.put(IntCell.TYPE, compatibleTypes);

        // DoubleCell
        compatibleTypes = new TreeSet<String>();
        compatibleTypes.add(SQL_TYPE_DOUBLE);
        map.put(DoubleCell.TYPE, compatibleTypes);

        // DateAndTimeCell
        compatibleTypes = new TreeSet<String>();
        compatibleTypes.add(SQL_TYPE_DATEANDTIME);
        map.put(DateAndTimeCell.TYPE, compatibleTypes);

        // BinaryObjectDataCell
        compatibleTypes = new TreeSet<String>();
        compatibleTypes.add(SQL_TYPE_BLOB);
        map.put(BinaryObjectDataCell.TYPE, compatibleTypes);

        // StringCell
        compatibleTypes = new TreeSet<String>();
        compatibleTypes.add(SQL_TYPE_STRING);
        map.put(StringCell.TYPE, compatibleTypes);

        return map;
    }

    static Map<DataType, Set<String>> getSqlTypesMap(final String dbIdentifier) {
        return getSqlTypesMap();
    }

    static List<String> getSqlTypes(final String dbIdentifier) {
        return getSqlTypes();
    }

    static List<String> getSqlTypes() {
        List<String> types = new ArrayList<String>();
        types.add(SQL_TYPE_BOOLEAN);
        types.add(SQL_TYPE_INTEGER);
        types.add(SQL_TYPE_DOUBLE);
        types.add(SQL_TYPE_DATEANDTIME);
        types.add(SQL_TYPE_BLOB);
        types.add(SQL_TYPE_STRING);
        return types;
    }

    static String getDefaultSQLType(final DataType knimeType) {
        if (knimeType.equals(BooleanCell.TYPE)) {
            return SQL_TYPE_BOOLEAN;
        } else if (knimeType.equals(IntCell.TYPE)) {
            return SQL_TYPE_INTEGER;
        } else if (knimeType.equals(DoubleCell.TYPE)) {
            return SQL_TYPE_DOUBLE;
        } else if (knimeType.equals(DateAndTimeCell.TYPE)) {
            return SQL_TYPE_DATEANDTIME;
        } else if (knimeType.equals(BinaryObjectDataCell.TYPE)) {
            return SQL_TYPE_BLOB;
        } else {
            return SQL_TYPE_STRING;
        }
    }

}
