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
 *   Jan 15, 2016 (budiyanto): created
 */
package org.knime.base.node.io.database.tablecreator.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.tablecreator.DBColumn;
import org.knime.core.node.port.database.tablecreator.DBKey;

/**
 * A configuration class to store the settings of DBTable
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBTableCreatorConfiguration {

    /** Default schema **/
    public static String DEFAULT_SCHEMA = "";

    /** Default table name **/
    public static String DEFAULT_TABLE_NAME = "newtable";

    /** Default temporary table value **/
    public static boolean DEFAULT_IS_TEMP_TABLE = false;

    /** Default drop existing table value **/
    public static boolean DEFAULT_IS_DROP_EXISTING = false;

    /** Configuration key for the schema **/
    public static final String CFG_SCHEMA = "Schema";

    /** Configuration key for the table name **/
    public static final String CFG_TABLE_NAME = "TableName";

    /** Configuration key for the temporary table **/
    public static final String CFG_TEMP_TABLE = "TempTable";

    /** Configuration key for the drop existing **/
    public static final String CFG_DROP_EXISTING = "DropExisting";

    /** Configuration key for the column definitions settings **/
    public static final String CFG_COLUMNS_SETTINGS = "Columns";

    /** Configuration key for the key definitions settings **/
    public static final String CFG_KEYS_SETTINGS = "Keys";

    /** Configuration key for the column name based type mapping **/
    public static final String CFG_NAME_BASED_TYPE_MAPPING = "NameBasedTypeMapping";

    /** Configuration key for the knime type based mapping **/
    public static final String CFG_KNIME_BASED_TYPE_MAPPING = "KnimeBasedTypeMapping";

    /** Configuration key for the column name based keys mapping **/
    public static final String CFG_NAME_BASED_KEYS = "NameBasedKeys";

    private final SettingsModelString m_settingsModelSchema = new SettingsModelString(CFG_SCHEMA, DEFAULT_SCHEMA);

    private final SettingsModelString m_settingsModelTableName = new SettingsModelString(CFG_TABLE_NAME, "");

    private final SettingsModelBoolean m_settingsModelTempTable =
        new SettingsModelBoolean(CFG_TEMP_TABLE, DEFAULT_IS_TEMP_TABLE);

    private final SettingsModelBoolean m_settingsModelDropExisting =
        new SettingsModelBoolean(CFG_DROP_EXISTING, DEFAULT_IS_DROP_EXISTING);

    private final Map<String, List<RowElement>> m_tableMap = new HashMap<String, List<RowElement>>();

    private final Map<String, SQLTypeCellEditor> m_sqlCellEditorMap = new HashMap<String, SQLTypeCellEditor>();

    private DataTableSpec m_tableSpec;

    private boolean m_isTableSpecChanged = false;

    private boolean m_settingsChangedInDialog = false;

    private DataTableSpec m_tempSpec;

    /**
     * Creates a new instance of DBTableCreatorConfiguration
     */
    public DBTableCreatorConfiguration() {
        m_tableMap.put(CFG_COLUMNS_SETTINGS, new ArrayList<RowElement>());
        m_tableMap.put(CFG_KEYS_SETTINGS, new ArrayList<RowElement>());
        m_tableMap.put(CFG_NAME_BASED_TYPE_MAPPING, new ArrayList<RowElement>());
        m_tableMap.put(CFG_KNIME_BASED_TYPE_MAPPING, new ArrayList<RowElement>());
        m_tableMap.put(CFG_NAME_BASED_KEYS, new ArrayList<RowElement>());

        m_settingsModelSchema.setEnabled(!m_settingsModelTempTable.getBooleanValue());
        m_settingsModelTempTable.addChangeListener(new ChangeListener() {

            @Override
            public void stateChanged(final ChangeEvent e) {
                m_settingsModelSchema.setEnabled(!m_settingsModelTempTable.getBooleanValue());
            }
        });
    }

    /**
     * Returns the SettingsModelString for the schema
     *
     * @return the SettingsModelString for the schema
     */
    public SettingsModelString getSettingsModelSchema() {
        return m_settingsModelSchema;
    }

    /**
     * Returns the SettingsModelString for the table name
     *
     * @return the SettingsModelString for the table name
     */
    public SettingsModelString getSettingsModelTableName() {
        return m_settingsModelTableName;
    }

    /**
     * Returns the SettingsModelBoolean for the temporary table
     *
     * @return the SettingsModelBoolean for the temporary table
     */
    public SettingsModelBoolean getSettingsModelTempTable() {
        return m_settingsModelTempTable;
    }

    /**
     * Returns the SettingsModelBoolean for the drop existing
     *
     * @return the SettingsModelBoolean for the drop existing
     */
    public SettingsModelBoolean getSettingsModelDropExisting() {
        return m_settingsModelDropExisting;
    }

    /**
     * Returns the schema
     *
     * @return the schema
     */
    public String getSchema() {
        return m_settingsModelSchema.getStringValue();
    }

    /**
     * Sets the schema
     *
     * @param schema the schema to set
     */
    public void setSchema(final String schema) {
        m_settingsModelSchema.setStringValue(schema);
    }

    /**
     * Returns the table name
     *
     * @return the table name
     */
    public String getTableName() {
        return m_settingsModelTableName.getStringValue();
    }

    /**
     * Sets the table name
     *
     * @param tableName the table name to set
     */
    public void setTableName(final String tableName) {
        m_settingsModelTableName.setStringValue(tableName);
    }

    /**
     * Returns true if the table is a temporary table, otherwise returns false
     *
     * @return true if the table is a temporary table, otherwise false
     */
    public boolean isTempTable() {
        return m_settingsModelTempTable.getBooleanValue();
    }

    /**
     * Sets to true if the table is a temporary table, otherwise set to false
     *
     * @param isTempTable true if the table is a temporary table, otherwise false
     */
    public void setTempTable(final boolean isTempTable) {
        m_settingsModelTempTable.setBooleanValue(isTempTable);
    }

    /**
     * Returns true if the existing table should be dropped, otherwise returns false
     *
     * @return true if the existing table should be dropped, otherwise false
     */
    public boolean isDropExisting() {
        return m_settingsModelDropExisting.getBooleanValue();
    }

    /**
     * Sets to true if the existing table should be dropped, otherwise sets to false
     *
     * @param isDropExisting true if the existing table should be dropped, otherwise false
     */
    public void setDropExisting(final boolean isDropExisting) {
        m_settingsModelDropExisting.setBooleanValue(isDropExisting);
    }

    /**
     * Returns the DataTableSpec instance
     *
     * @return the DataTableSpec instance
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

    /**
     * Returns the columns
     *
     * @return the columns
     */
    public List<DBColumn> getColumns() {
        List<DBColumn> columns = new ArrayList<DBColumn>();
        for (RowElement el : getRowElements(CFG_COLUMNS_SETTINGS)) {
            ColumnElement col = (ColumnElement)el;
            columns.add(col.getDBColumn());
        }
        return columns;
    }

    /**
     * Returns the keys
     *
     * @return the keys
     */
    public List<DBKey> getKeys() {
        List<DBKey> keys = new ArrayList<DBKey>();
        for (RowElement el : getRowElements(CFG_KEYS_SETTINGS)) {
            KeyElement elem = (KeyElement)el;
            keys.add(elem.getDBKey());
        }
        return keys;
    }

    /**
     * Returns the list of RowElements retrieved from the map using the specified key
     *
     * @param cfgKey key used to retrieve the RowElements
     * @return the list of RowElements retrieved from the map
     */
    public List<RowElement> getRowElements(final String cfgKey) {
        List<RowElement> elems = m_tableMap.get(cfgKey);
        if (elems == null) {
            elems = new ArrayList<RowElement>();
            m_tableMap.put(cfgKey, elems);
        }
        return elems;
    }

    /**
     * Returns the SQLTypeCellEditor retrieved from the map using the specified key
     *
     * @param editorKey key used to retrieve the SQLTypeCellEditor
     * @param relatedColumn related column for the SQLTypeCellEditor
     * @return the SQLTypeCellEditor retrieved from the map
     */
    public SQLTypeCellEditor getSqlTypeCellEditor(final String editorKey, final int relatedColumn) {
        SQLTypeCellEditor editor = m_sqlCellEditorMap.get(editorKey);
        if (editor == null) {
            editor = new SQLTypeCellEditor(relatedColumn);
            m_sqlCellEditorMap.put(editorKey, editor);
        }
        return editor;
    }

    /**
     * Set table spec instance to the input table spec, null is not allowed
     *
     * @param spec input table spec
     * @return true if the table spec instance is changed, otherwise false
     */
    public boolean setTableSpec(final DataTableSpec spec) {
        if ((spec == null) || (m_tableSpec != null && m_tableSpec.equalStructure(spec))) {
            m_isTableSpecChanged = false;
        } else {
            m_tableSpec = spec;
            m_isTableSpecChanged = true;
        }

        return m_isTableSpecChanged;
    }

    public void setTableSpec2(final DataTableSpec spec) {
        m_tableSpec = spec;
    }

    /**
     * Returns true if the table spec has changed
     *
     * @return true if the table spec has changed, otherwise false
     */
    public boolean isTableSpecChanged() {
        return m_isTableSpecChanged;
    }

    public boolean isNewTableSpec(final DataTableSpec spec) {
        if((spec == null) || (m_tableSpec != null && m_tableSpec.equalStructure(spec))) {
            return false;
        }

        return true;
    }

    /**
     * Load settings for NodeModel
     *
     * @param settings NodeSettingsRO instance to load from
     * @throws InvalidSettingsException
     */
    public void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }

        loadSettingsForSettingsModel(settings);
        loadSettingsForRowElements(CFG_NAME_BASED_TYPE_MAPPING, settings);
        loadSettingsForRowElements(CFG_KNIME_BASED_TYPE_MAPPING, settings);
        loadSettingsForRowElements(CFG_NAME_BASED_KEYS, settings);
        loadSettingsForRowElements(CFG_COLUMNS_SETTINGS, settings);
        loadSettingsForRowElements(CFG_KEYS_SETTINGS, settings);

//        m_settingsChangedInDialog = true;
    }

    /**
     * Load settings for NodeDialog
     *
     * @param settings NodeSettingsRO instance to load from
     * @param specs PortObjectSpec array to load from
     * @throws InvalidSettingsException
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws InvalidSettingsException {
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }

        loadSettingsForSettingsModel(settings);
        loadSettingsForRowElements(CFG_NAME_BASED_TYPE_MAPPING, settings);
        loadSettingsForRowElements(CFG_KNIME_BASED_TYPE_MAPPING, settings);
        loadSettingsForRowElements(CFG_NAME_BASED_KEYS, settings);

        final String dbIdentifier = ((DatabaseConnectionPortObjectSpec)specs[0]).getDatabaseIdentifier();
        loadSettingsForSqlEditor(dbIdentifier);

        DataTableSpec spec = (DataTableSpec)specs[1];
        if (setTableSpec(spec)) {
            loadSettingsFromTableSpec(spec);
        } else {
            loadSettingsForRowElements(CFG_COLUMNS_SETTINGS, settings);
            loadSettingsForRowElements(CFG_KEYS_SETTINGS, settings);
        }
    }

    /**
     * Load settings for NodeDialog
     *
     * @param settings NodeSettingsRO instance to load from
     * @param specs PortObjectSpec array to load from
     * @throws InvalidSettingsException
     */
    public void loadSettingsForDialog2(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws InvalidSettingsException {
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }

        loadSettingsForSettingsModel(settings);
        loadSettingsForRowElements(CFG_NAME_BASED_TYPE_MAPPING, settings);
        loadSettingsForRowElements(CFG_KNIME_BASED_TYPE_MAPPING, settings);
        loadSettingsForRowElements(CFG_NAME_BASED_KEYS, settings);

        final String dbIdentifier = ((DatabaseConnectionPortObjectSpec)specs[0]).getDatabaseIdentifier();
        loadSettingsForSqlEditor(dbIdentifier);

        DataTableSpec spec = (DataTableSpec)specs[1];

        if(isNewTableSpec(spec)) {
            loadSettingsFromTableSpec(spec);
            m_tempSpec = spec;
        } else {
            loadSettingsForRowElements(CFG_COLUMNS_SETTINGS, settings);
            loadSettingsForRowElements(CFG_KEYS_SETTINGS, settings);
        }

//        if (setTableSpec(spec)) {
//            loadSettingsFromTableSpec(spec);
//        } else {
//            loadSettingsForRowElements(CFG_COLUMNS_SETTINGS, settings);
//            loadSettingsForRowElements(CFG_KEYS_SETTINGS, settings);
//        }
    }

    /**
     * A helper method to load settings for all SettingsModel instances
     *
     * @param settings NodeSettingsRO instance to load from
     * @throws InvalidSettingsException
     */
    private void loadSettingsForSettingsModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settingsModelTableName.loadSettingsFrom(settings);
        m_settingsModelTempTable.loadSettingsFrom(settings);
        m_settingsModelSchema.setEnabled(!m_settingsModelTempTable.getBooleanValue());
        m_settingsModelSchema.loadSettingsFrom(settings);
        m_settingsModelDropExisting.loadSettingsFrom(settings);

        if (getTableName().isEmpty()) {
            setTableName(DEFAULT_TABLE_NAME);
        }
    }

    /**
     * A helper method to load settings for all RowElements
     *
     * @param cfgKey key used to retrieve the RowElement list from the map
     * @param settings NodeSettingsRO instance to load from
     * @throws InvalidSettingsException
     */
    private void loadSettingsForRowElements(final String cfgKey, final NodeSettingsRO settings)
        throws InvalidSettingsException {
        final NodeSettingsRO root = settings.getNodeSettings(cfgKey);
        List<RowElement> elements = m_tableMap.get(cfgKey);
        elements.clear();
        for (String settingsKey : root.keySet()) {
            final NodeSettingsRO cfg = root.getNodeSettings(settingsKey);
            final RowElement elem = createRowElement(cfgKey, cfg);
            if (elem != null) {
                elements.add(elem);
            }
        }
    }

    /**
     * A helper method to create a new instance of RowElement from NodeSettingsRO instance
     *
     * @param cfgKey key to determine which kind of RowElement to create
     * @param settings NodeSettingsRO instance used to create a new RowElement
     * @return a new instance of RowElement
     */
    private RowElement createRowElement(final String cfgKey, final NodeSettingsRO settings) {
        switch (cfgKey) {
            case CFG_COLUMNS_SETTINGS:
                return new ColumnElement(settings);
            case CFG_KEYS_SETTINGS:
                KeyElement elem = new KeyElement(settings);
                Set<ColumnElement> columns = new HashSet<ColumnElement>();
                for (DBColumn dbCol : elem.getColumns()) {
                    boolean isFound = false;
                    for (RowElement el : getRowElements(CFG_COLUMNS_SETTINGS)) {
                        ColumnElement colElem = (ColumnElement)el;
                        if (dbCol.getName().equalsIgnoreCase(colElem.getName())) {
                            columns.add(colElem);
                            isFound = true;
                            break;
                        }
                    }
                    if (!isFound) {
                        throw new IllegalArgumentException(String.format("Column '%s' is undefined", dbCol.getName()));
                    }
                }
                elem.setColumnElements(columns);
                return elem;
            case CFG_NAME_BASED_TYPE_MAPPING:
                return new NameBasedMappingElement(settings);
            case CFG_KNIME_BASED_TYPE_MAPPING:
                return new KnimeBasedMappingElement(settings);
            case CFG_NAME_BASED_KEYS:
                return new NameBasedKeysElement(settings);
            default:
                return null;
        }
    }

    /**
     * Validate settings
     * @param settings settings to validate
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException{
        m_settingsModelSchema.validateSettings(settings);
        m_settingsModelTableName.validateSettings(settings);
        m_settingsModelTempTable.validateSettings(settings);
        m_settingsModelDropExisting.validateSettings(settings);
        for(String key : m_tableMap.keySet()){
            settings.getNodeSettings(key);
        }
    }

    /**
     * Saves settings for NodeModel
     *
     * @param settings NodeSettingsWO instance to save settings to
     */
    public void saveSettingsForModel(final NodeSettingsWO settings) {
        m_settingsModelSchema.saveSettingsTo(settings);
        m_settingsModelTableName.saveSettingsTo(settings);
        m_settingsModelTempTable.saveSettingsTo(settings);
        m_settingsModelDropExisting.saveSettingsTo(settings);
        for (Entry<String, List<RowElement>> entry : m_tableMap.entrySet()) {
            final NodeSettingsWO root = settings.addNodeSettings(entry.getKey());
            int idx = 0;
            for (RowElement elem : entry.getValue()) {
                final NodeSettingsWO cfg = root.addNodeSettings(elem.getPrefix() + idx++);
                elem.saveSettingsTo(cfg);
            }
        }
    }

    /**
     * Saves settings for NodeDialog
     *
     * @param settings NodeSettingsWO instance to save settings to
     * @throws InvalidSettingsException
     */
    public void saveSettingsForDialog(final NodeSettingsWO settings) throws InvalidSettingsException {
        saveSettingsForModel(settings);
        saveSettingsForSqlEditor();
        if(isNewTableSpec(m_tempSpec)) {
            m_tableSpec = m_tempSpec;
            m_tempSpec = null;
            System.out.println(m_tableSpec.getNumColumns());
        }
    }

    /**
     * A helper method to save settings of SQLTypeCellEditor to StringHistory
     */
    private void saveSettingsForSqlEditor() {
        for (Entry<String, SQLTypeCellEditor> entry : m_sqlCellEditorMap.entrySet()) {
            entry.getValue().saveSettings();
        }
    }

    /**
     * A helper method to load settings of SQLTypeCellEditor from StringHistory
     *
     * @param identifier identifier used to get the corresponding StringHistory
     */
    private void loadSettingsForSqlEditor(final String identifier) {
        for (Entry<String, SQLTypeCellEditor> entry : m_sqlCellEditorMap.entrySet()) {
            entry.getValue().loadSettings(entry.getKey() + "_" + identifier);
        }
    }

    /**
     * Load column settings from table spec
     *
     * @param spec DataTableSpec to load from
     */
    public void loadSettingsFromTableSpec(final DataTableSpec spec) {

        List<RowElement> colElems = getRowElements(CFG_COLUMNS_SETTINGS);
        colElems.clear();

        List<RowElement> nameBasedMappingElems = getRowElements(CFG_NAME_BASED_TYPE_MAPPING);
        for (int colIdx = 0; colIdx < spec.getNumColumns(); colIdx++) {
            DataColumnSpec colSpec = spec.getColumnSpec(colIdx);
            final String name = colSpec.getName();
            boolean isMatched = false;
            String type = null;
            boolean notNull = false;

            for (RowElement el : nameBasedMappingElems) {
                final NameBasedMappingElement elem = (NameBasedMappingElement)el;
                final Pattern pattern =
                    PatternUtil.compile(elem.getNamePattern(), elem.isRegex(), Pattern.CASE_INSENSITIVE);
                final Matcher matcher = pattern.matcher(name);
                if (matcher.matches()) {
                    type = elem.getSqlType();
                    notNull = elem.isNotNull();
                    isMatched = true;
                    break;
                }
            }

            List<RowElement> knimeBasedMappingElems = getRowElements(CFG_KNIME_BASED_TYPE_MAPPING);
            if (!isMatched) { // No match on name-based mapping, try to look at Knime-based mapping
                for (RowElement el : knimeBasedMappingElems) {
                    final KnimeBasedMappingElement elem = (KnimeBasedMappingElement)el;
                    if (elem.getKnimeType().equals(colSpec.getType())) {
                        type = elem.getSqlType();
                        notNull = elem.isNotNull();
                        isMatched = true;
                        break;
                    }
                }
            }

            if (!isMatched) { // No match on name-based and knime-based mapping, use default value
                type = DBUtil.getDefaultSQLType(colSpec.getType());
            }

            final ColumnElement elem = new ColumnElement(name, type, notNull);
            colElems.add(elem);
        }

        updateKeys();

    }

    /**
     * A helper method to update the keys with dynamic settings if available
     */
    private void updateKeys() {
        List<RowElement> keyElems = getRowElements(CFG_KEYS_SETTINGS);
        keyElems.clear();
        List<RowElement> nameBasedKeyElems = getRowElements(CFG_NAME_BASED_KEYS);
        if (!nameBasedKeyElems.isEmpty()) {
            List<RowElement> colElems = getRowElements(CFG_COLUMNS_SETTINGS);
            for (RowElement el : nameBasedKeyElems) {
                NameBasedKeysElement nameBasedKeyElem = (NameBasedKeysElement)el;
                // Looking for columns that match the search pattern
                Set<ColumnElement> columns = new HashSet<ColumnElement>();
                final String searchPattern = nameBasedKeyElem.getNamePattern();
                final Pattern pattern =
                    PatternUtil.compile(searchPattern, nameBasedKeyElem.isRegex(), Pattern.CASE_INSENSITIVE);
                for (RowElement re : colElems) {
                    ColumnElement colElem = (ColumnElement)re;
                    final Matcher m = pattern.matcher(colElem.getName());
                    if (m.matches()) {
                        columns.add(colElem);
                    }
                }

                // Add a new key if there is at least one column that matches the search pattern
                if (!columns.isEmpty()) {
                    final String keyName = nameBasedKeyElem.getKeyName();
                    final boolean primaryKey = nameBasedKeyElem.isPrimaryKey();
                    final KeyElement newElem = new KeyElement(keyName, columns, primaryKey);
                    keyElems.add(newElem);
                }
            }
        }
    }

    /**
     * Return the prefix of RowElement
     *
     * @param cfgKey key to identify the kind of RowElement
     * @return the prefix of RowElement
     */
    static String getPrefix(final String cfgKey) {
        switch (cfgKey) {
            case CFG_COLUMNS_SETTINGS:
                return ColumnElement.DEFAULT_PREFIX;
            case CFG_KEYS_SETTINGS:
                return KeyElement.DEFAULT_PREFIX;
            case CFG_NAME_BASED_TYPE_MAPPING:
                return NameBasedMappingElement.DEFAULT_PREFIX;
            case CFG_KNIME_BASED_TYPE_MAPPING:
                return KnimeBasedMappingElement.DEFAULT_PREFIX;
            case CFG_NAME_BASED_KEYS:
                return NameBasedKeysElement.DEFAULT_PREFIX;
            default:
                return null;
        }
    }

}
