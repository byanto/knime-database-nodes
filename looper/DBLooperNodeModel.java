package org.knime.base.node.io.database.looper;

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
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;

/**
 * This is the model implementation of DBLooper.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBLooperNodeModel extends DBNodeModel implements FlowVariableProvider{

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DBLooperNodeModel.class);

    static final boolean DEF_APPEND_INPUT_COL = true;

    static final boolean DEF_INCLUDE_EMPTY_RESULTS = false;

    static final boolean DEF_RETAIN_ALL_COLUMNS = false;

    static final String CFG_SQL_STATEMENT = "sql_statement";

    static final String INPUT_COLUMNS_PLACEHOLDER = "input_columns";

    private final SettingsModelBoolean m_appendInputColumnsModel = createAppendInputColsModel();

    private final SettingsModelBoolean m_includeEmptyResultsModel = createIncludeEmptyResultsModel();

    private final SettingsModelBoolean m_retainAllColumns = createRetainAllColumnsModel();

    private String m_sqlStatement = getDefaultSQLStatement();

    static SettingsModelBoolean createAppendInputColsModel() {
        return new SettingsModelBoolean("append_input_columns", DEF_APPEND_INPUT_COL);
    }

    static SettingsModelBoolean createIncludeEmptyResultsModel() {
        return new SettingsModelBoolean("include_empty_results", DEF_INCLUDE_EMPTY_RESULTS);
    }

    static SettingsModelBoolean createRetainAllColumnsModel() {
        return new SettingsModelBoolean("retain_all_columns", DEF_RETAIN_ALL_COLUMNS);
    }

    /**
     * Constructor for the node model.
     */
    protected DBLooperNodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE, DatabasePortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec)
            throws Exception {

        final String newQuery = parseSQLStatement(m_sqlStatement);

        LOGGER.debug("SQL Statement: " + newQuery);

        if(m_appendInputColumnsModel.getBooleanValue()) {

        }

        return new BufferedDataTable[]{(BufferedDataTable)inData[0]};
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

        final DataTableSpec inSpec = (DataTableSpec) inSpecs[0];
        if(inSpec.getNumColumns() < 1) {
            throw new InvalidSettingsException("No column spec available.");
        }

        validateColumns(inSpec, m_sqlStatement);

//        final DatabasePortObjectSpec dbSpec = (DatabasePortObjectSpec) inSpecs[1];
//        DatabaseQueryConnectionSettings conn = dbSpec.getConnectionSettings(getCredentialsProvider());
//        String newQuery = parseSQLStatement(conn.getQuery());
//
//        LOGGER.debug("Original Query: " + newQuery);
//
//        conn = createDBQueryConnection(dbSpec, newQuery);

        return new DataTableSpec[1];







//        DataTableSpec outSpec = null;
//        if(m_retainAllColumns.getBooleanValue()){
//            outSpec = (DataTableSpec) inSpecs[0];
//        }


    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_appendInputColumnsModel.saveSettingsTo(settings);
        m_includeEmptyResultsModel.saveSettingsTo(settings);
        m_retainAllColumns.saveSettingsTo(settings);
        settings.addString(CFG_SQL_STATEMENT, m_sqlStatement);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_appendInputColumnsModel.loadSettingsFrom(settings);
        m_includeEmptyResultsModel.loadSettingsFrom(settings);
        m_retainAllColumns.loadSettingsFrom(settings);
        m_sqlStatement = settings.getString(CFG_SQL_STATEMENT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_appendInputColumnsModel.validateSettings(settings);
        m_includeEmptyResultsModel.validateSettings(settings);
        m_retainAllColumns.validateSettings(settings);
        final String sqlStatement = settings.getString(CFG_SQL_STATEMENT);
        if(sqlStatement != null && !sqlStatement.contains(
                DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER)){
            throw new InvalidSettingsException("Database table place holder ("
                + DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER
                + ") must not be replaced.");
        }
    }

    /**
     * @return the default source code
     */
    static String getDefaultSQLStatement() {
        return "SELECT * FROM " + DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER;
    }

    static String getColumnPlaceHolder(final DataColumnSpec colSpec) {
        return "${" + colSpec.getName() + "}$";
    }

    /**
     * Parses the given SQL query and resolves the table placeholder and
     * variables.
     * @param query the query used to replace the table placeholder
     * (the incoming query from the Database Connector)
     */
    private String parseSQLStatement(final String query) {
//        final StringBuilder builder = new StringBuilder();
//        final String[] inQueris = query.split(DBReader.SQL_QUERY_SEPARATOR);
//        String inSelect = inQueris[inQueris.length - 1];
//        for(int i = 0; i < inQueris.length; i++) {
//            builder.append(inQueris[i]);
//            builder.append(DBReader.SQL_QUERY_SEPARATOR);
//        }

        // Replace the "#table#" placeholder with the input query
        String resultQuery = m_sqlStatement.replaceAll
                (DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER, "(" + query + ")");

        // Replace the flowVariable placeholder with the actual value
        resultQuery = FlowVariableResolver.parse(resultQuery, this);

        // Replace the column placeholder with '?'
        resultQuery = resultQuery.replaceAll("\\$\\{(.*?)\\}\\$", "?");

        return resultQuery;

//        builder.append(resultQuery);
//
//        return builder.toString();

    }

    private void validateColumns(final DataTableSpec spec, final String query)
            throws InvalidSettingsException {
        final Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}\\$");
        final Matcher matcher = pattern.matcher(query);
        while(matcher.find()) {
            final String col = matcher.group(1);
            if(!spec.containsName(col)) {
                throw new InvalidSettingsException("Cannot find column " + col
                    + " in the input table.");
            }
        }
    }

}
