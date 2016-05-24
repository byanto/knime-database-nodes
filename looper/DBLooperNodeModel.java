package org.knime.base.node.io.database.looper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.base.node.io.database.DBNodeModel;
import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.reader.DBLooper;
import org.knime.core.node.streamable.DataTableRowInput;

/**
 * This is the model implementation of DBLooper.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBLooperNodeModel extends DBNodeModel implements FlowVariableProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DBLooperNodeModel.class);

    static final boolean DEF_APPEND_INPUT_COL = true;

    static final boolean DEF_INCLUDE_EMPTY_RESULTS = false;

    static final boolean DEF_RETAIN_ALL_COLUMNS = false;

    static final boolean DEF_FAIL_IF_EXCEPTION = false;

    static final String CFG_SQL_STATEMENT = "sql_statement";

    static final String INPUT_COLUMNS_PLACEHOLDER = "input_columns";

    private final SettingsModelBoolean m_appendInputColumnsModel = createAppendInputColsModel();

    private final SettingsModelBoolean m_includeEmptyResultsModel = createIncludeEmptyResultsModel();

    private final SettingsModelBoolean m_retainAllColumnsModel = createRetainAllColumnsModel();

    private final SettingsModelBoolean m_failIfExceptionModel = createFailIfExceptionModel();

    private String m_sqlStatement = getDefaultSQLStatement();

    private List<String> m_dataColumns = new ArrayList<String>();

    static SettingsModelBoolean createAppendInputColsModel() {
        return new SettingsModelBoolean("append_input_columns", DEF_APPEND_INPUT_COL);
    }

    static SettingsModelBoolean createIncludeEmptyResultsModel() {
        return new SettingsModelBoolean("include_empty_results", DEF_INCLUDE_EMPTY_RESULTS);
    }

    static SettingsModelBoolean createRetainAllColumnsModel() {
        return new SettingsModelBoolean("retain_all_columns", DEF_RETAIN_ALL_COLUMNS);
    }

    static SettingsModelBoolean createFailIfExceptionModel() {
        return new SettingsModelBoolean("fail_if_exception", DEF_FAIL_IF_EXCEPTION);
    }

    /**
     * Constructor for the node model.
     */
    protected DBLooperNodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE, DatabasePortObject.TYPE},
            new PortType[]{BufferedDataTable.TYPE, BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        final BufferedDataTable inTable = (BufferedDataTable)inData[0];
        final DatabasePortObject dbObject = (DatabasePortObject)inData[1];

        DatabaseQueryConnectionSettings conn = dbObject.getConnectionSettings(getCredentialsProvider());

        final String newQuery = parseSQLStatement(conn.getQuery());

        LOGGER.debug("SQL Statement: " + newQuery);

        conn = createDBQueryConnection(dbObject.getSpec(), newQuery);
        final DBLooper looper = conn.getUtility().getLooper(conn);

        final DataTableRowInput data = new DataTableRowInput(inTable);

        final BufferedDataTable outTable = looper
            .loopTable(exec, getCredentialsProvider(), data, inTable.size(), m_failIfExceptionModel.getBooleanValue(),
                m_appendInputColumnsModel.getBooleanValue(), m_includeEmptyResultsModel.getBooleanValue(),
                m_retainAllColumnsModel.getBooleanValue(), m_dataColumns.toArray(new String[m_dataColumns.size()]))
            .getDataTable();

        final BufferedDataTable errorTable = looper.getErrorDataTable();

        return new BufferedDataTable[]{outTable, errorTable};

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final DataTableSpec inSpec = (DataTableSpec)inSpecs[0];
        if (inSpec.getNumColumns() < 1) {
            throw new InvalidSettingsException("No column spec available.");
        }

        if ((inSpecs[1] == null) || !(inSpecs[1] instanceof DatabasePortObjectSpec)) {
            throw new InvalidSettingsException("No valid database connection available.");
        }

        parseDataColumns(m_sqlStatement);

        return new DataTableSpec[]{null};

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_appendInputColumnsModel.saveSettingsTo(settings);
        m_includeEmptyResultsModel.saveSettingsTo(settings);
        m_retainAllColumnsModel.saveSettingsTo(settings);
        m_failIfExceptionModel.saveSettingsTo(settings);
        settings.addString(CFG_SQL_STATEMENT, m_sqlStatement);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_appendInputColumnsModel.loadSettingsFrom(settings);
        m_includeEmptyResultsModel.loadSettingsFrom(settings);
        m_retainAllColumnsModel.loadSettingsFrom(settings);
        m_failIfExceptionModel.loadSettingsFrom(settings);
        m_sqlStatement = settings.getString(CFG_SQL_STATEMENT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_appendInputColumnsModel.validateSettings(settings);
        m_includeEmptyResultsModel.validateSettings(settings);
        m_retainAllColumnsModel.validateSettings(settings);
        m_failIfExceptionModel.validateSettings(settings);
        //        final String sqlStatement = settings.getString(CFG_SQL_STATEMENT);
        //        if(sqlStatement != null && !sqlStatement.contains(
        //                DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER)){
        //            throw new InvalidSettingsException("Database table place holder ("
        //                + DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER
        //                + ") must not be replaced.");
        //        }
    }

    /**
     * @return the default source code
     */
    static String getDefaultSQLStatement() {
        return "SELECT * FROM " + DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER;
    }

    static String getColumnPlaceHolder(final DataColumnSpec colSpec) {
        return "#{" + colSpec.getName() + "}#";
    }

    /**
     * Parses the given SQL query and resolves the table placeholder and variables.
     *
     * @param query the query used to replace the table placeholder (the incoming query from the Database Connector)
     */
    private String parseSQLStatement(final String query) throws InvalidSettingsException {

        // Replace the "#table#" placeholder with the input query
        String resultQuery =
            m_sqlStatement.replaceAll(DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER, "(" + query + ")");

        // Replace the flowVariable placeholder with the actual value
        resultQuery = FlowVariableResolver.parse(resultQuery, this);

        // Parse the data column and replace with "?"
        resultQuery = parseDataColumns(resultQuery);

        return resultQuery;

    }

    private String parseDataColumns(final String query) throws InvalidSettingsException {
        m_dataColumns.clear();
        final Pattern pattern = Pattern.compile("#{1}\\{(.*?)\\}#{1}");
        final Matcher matcher = pattern.matcher(query);
        while (matcher.find()) {
            final String col = matcher.group(1);
            m_dataColumns.add(col);
        }

        return matcher.replaceAll("?");
    }

}
