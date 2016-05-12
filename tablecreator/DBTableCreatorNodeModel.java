package org.knime.base.node.io.database.tablecreator;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.knime.base.node.io.database.DBNodeModel;
import org.knime.base.node.io.database.tablecreator.util.DBTableCreatorConfiguration;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;

/**
 * This is the model implementation of DBTableCreator.
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBTableCreatorNodeModel extends DBNodeModel {

    private static final String FLOW_VARIABLE_SCHEMA = "schema";

    private static final String FLOW_VARIABLE_TABLE_NAME = "tableName";

    private final DBTableCreatorConfiguration m_config = new DBTableCreatorConfiguration();

    /**
     * Constructor for the node model.
     */
    protected DBTableCreatorNodeModel() {
        super(new PortType[]{DatabaseConnectionPortObject.TYPE, BufferedDataTable.TYPE_OPTIONAL},
            new PortType[]{FlowVariablePortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {

        final DatabaseConnectionPortObject dbObject = (DatabaseConnectionPortObject)inData[0];
        final DatabaseConnectionSettings conn = dbObject.getSpec().getConnectionSettings(getCredentialsProvider());
        final Connection sqlConn = conn.createConnection(getCredentialsProvider());
        final DatabaseUtility dbUtility = conn.getUtility();
        final StatementManipulator manipulator = dbUtility.getStatementManipulator();

        // Set schema to empty if the table is a temporary table
        // A temporary table cannot have schema
        final String schema = m_config.isTempTable() ? "" : m_config.getSchema();
        final String tableName = (m_config.getTableName() == null || m_config.getTableName().isEmpty())
            ? DBTableCreatorConfiguration.DEFAULT_TABLE_NAME : m_config.getTableName();

        // Prepend schema to the table name if it is available
        String schemaTable = (schema == null || schema.isEmpty()) ? tableName : schema + "." + tableName;

        synchronized (conn.syncConnection(sqlConn)) {
            if (dbUtility.tableExists(sqlConn, manipulator.quoteIdentifier(schemaTable))) {
                if (m_config.isDropExisting()) {
                    try {
                        final String query = dbUtility.generateDropTableQuery(schema, tableName);
                        conn.execute(query, getCredentialsProvider());
                    } catch (SQLException ex) {
                        Throwable cause = ExceptionUtils.getRootCause(ex);
                        if (cause == null) {
                            cause = ex;
                        }
                        throw new InvalidSettingsException(
                            "Error while validating drop statement: " + cause.getMessage(), ex);
                    }
                } else {
                    throw new InvalidSettingsException("Table \"" + schemaTable
                        + "\" exists in database, set option \"Drop existing table\" to drop it.");
                }
            }

            // Create a new table
            try {
                if (m_config.isTableSpecChanged()) {
                    m_config.loadSettingsFromTableSpec(m_config.getTableSpec());
                }

                final String query = conn.getUtility().generateCreateTableQuery(schema,
                    tableName, m_config.isTempTable(), m_config.getColumns(), m_config.getKeys());
                conn.execute(query, getCredentialsProvider());
            } catch (SQLException ex) {
                Throwable cause = ExceptionUtils.getRootCause(ex);
                if (cause == null) {
                    cause = ex;
                }
                throw new InvalidSettingsException("Error while validating create statement: " + cause.getMessage(),
                    ex);
            }
        }

        pushFlowVariableString(FLOW_VARIABLE_SCHEMA, schema);
        pushFlowVariableString(FLOW_VARIABLE_TABLE_NAME, tableName);
        return new PortObject[]{FlowVariablePortObject.INSTANCE};
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
        final DataTableSpec spec = (DataTableSpec)inSpecs[1];
        final boolean isColumnsEmpty = m_config.getColumns().isEmpty();

        if (spec == null && isColumnsEmpty) {
            throw new InvalidSettingsException("At least one column must be defined.");
        }

        if (m_config.setTableSpec(spec) && !isColumnsEmpty) {
            setWarningMessage("Column settings are not empty. They will be overwritten by the table spec.");
        }

        if(!m_config.isTableSpecChanged() && isColumnsEmpty){
            throw new InvalidSettingsException("At least one column must be defined.");
        }

        return new FlowVariablePortObjectSpec[]{FlowVariablePortObjectSpec.INSTANCE};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_config.saveSettingsForModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config.loadSettingsForModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_config.validateSettings(settings);
//        m_config.loadSettingsForModel(settings);
    }
}
