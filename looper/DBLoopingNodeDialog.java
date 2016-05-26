package org.knime.base.node.io.database.looper;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.port.PortObjectSpec;

/**
 * <code>NodeDialog</code> for the "DBLooper" Node.
 *
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBLoopingNodeDialog extends NodeDialogPane {

    private DBLoopingPanel m_panel = new DBLoopingPanel();

    /**
     * New pane for configuring the DBLooper node.
     */
    protected DBLoopingNodeDialog() {
        addTab("DB Looper", m_panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        m_panel.saveSettingsTo(settings);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {

        final DataTableSpec inSpec = (DataTableSpec) specs[0];
        if(inSpec.getNumColumns() < 1) {
            throw new NotConfigurableException("No column spec available");
        }

        if(specs[1] == null){
            throw new NotConfigurableException("No valid database connection available.");
        }

        m_panel.loadSettingsFrom(settings, specs, getAvailableFlowVariables().values());

    }
}

