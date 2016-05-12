package org.knime.base.node.io.database.looper;

import java.io.File;
import java.io.IOException;

import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
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
public class DBLooperNodeModel extends NodeModel implements FlowVariableProvider{

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DBLooperNodeModel.class);

    static final boolean DEF_APPEND_INPUT_COL = true;

    static final boolean DEF_INCLUDE_EMPTY_RESULTS = false;

    static final boolean DEF_RETAIN_ALL_COLUMNS = false;

    static final String CFG_SQL_STATEMENT = "sql_statement";

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

        final String statement = FlowVariableResolver.parse(m_sqlStatement, this);
        LOGGER.debug("SQL Statement: " + statement);

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

        DataTableSpec outSpec = null;
        if(m_retainAllColumns.getBooleanValue()){
            outSpec = (DataTableSpec) inSpecs[0];
        }

        return new DataTableSpec[]{outSpec};
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
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // TODO: generated method stub
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // TODO: generated method stub
    }

    /**
     * @return the default source code
     */
    static String getDefaultSQLStatement() {
        return "SELECT * FROM " + DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER;
    }

}
