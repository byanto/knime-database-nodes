package org.knime.base.node.io.database.tablecreator;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;

import org.knime.base.node.io.database.tablecreator.util.ColumnsPanel;
import org.knime.base.node.io.database.tablecreator.util.DBTableCreatorConfiguration;
import org.knime.base.node.io.database.tablecreator.util.KeysPanel;
import org.knime.base.node.io.database.tablecreator.util.KnimeBasedMappingPanel;
import org.knime.base.node.io.database.tablecreator.util.NameBasedKeysPanel;
import org.knime.base.node.io.database.tablecreator.util.NameBasedMappingPanel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;

/**
 * <code>NodeDialog</code> for the "DBTableCreator" Node.
 *
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows creation of a simple dialog with standard
 * components. If you need a more complex dialog please derive directly from {@link org.knime.core.node.NodeDialogPane}.
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBTableCreatorNodeDialog extends NodeDialogPane {

    private static final int PANEL_DEFAULT_WIDTH = 720;

    private static final int PANEL_DEFAULT_HEIGHT = 380;

    private final DBTableCreatorConfiguration m_config = new DBTableCreatorConfiguration();

    private final ColumnsPanel m_columnsPanel;

    private final KeysPanel m_keysPanel;

    private final NameBasedMappingPanel m_nameBasedMappingPanel;

    private final KnimeBasedMappingPanel m_knimeTypeBasedMappingPanel;

    private final NameBasedKeysPanel m_nameBasedKeysPanel;

    /**
     * New pane for configuring the DBTableCreator node.
     */
    protected DBTableCreatorNodeDialog() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BorderLayout());
        panel.setPreferredSize(new Dimension(PANEL_DEFAULT_WIDTH, PANEL_DEFAULT_HEIGHT));

        final JTabbedPane tabs = new JTabbedPane();

        m_columnsPanel = new ColumnsPanel(DBTableCreatorConfiguration.CFG_COLUMNS_SETTINGS, m_config);
        m_keysPanel = new KeysPanel(DBTableCreatorConfiguration.CFG_KEYS_SETTINGS, m_config);
        m_nameBasedMappingPanel =
            new NameBasedMappingPanel(DBTableCreatorConfiguration.CFG_NAME_BASED_TYPE_MAPPING, m_config);
        m_knimeTypeBasedMappingPanel =
            new KnimeBasedMappingPanel(DBTableCreatorConfiguration.CFG_KNIME_BASED_TYPE_MAPPING, m_config);
        m_nameBasedKeysPanel = new NameBasedKeysPanel(DBTableCreatorConfiguration.CFG_NAME_BASED_KEYS, m_config);

        tabs.add("Table", createTableSettingsPanel());
        tabs.add(m_columnsPanel.getTitle(), m_columnsPanel);
        tabs.add(m_keysPanel.getTitle(), m_keysPanel);
        tabs.add("Dynamic Type Settings", createDynamicPanel());
        tabs.add(m_nameBasedKeysPanel.getTitle(), m_nameBasedKeysPanel);

        panel.add(tabs);
        super.addTab("Settings", panel);

    }

    private JPanel createTableSettingsPanel() {
        final JPanel panel = new JPanel();

        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        final Box box = new Box(BoxLayout.Y_AXIS);
        panel.add(box);

        box.add(Box.createVerticalGlue());

        final DialogComponentString schemaComp =
            new DialogComponentString(m_config.getSettingsModelSchema(), "Schema: ");
        box.add(schemaComp.getComponentPanel());
        box.add(Box.createVerticalGlue());

        final DialogComponentString tableNameComp =
            new DialogComponentString(m_config.getSettingsModelTableName(), "Table name: ");
        box.add(tableNameComp.getComponentPanel());
        box.add(Box.createVerticalGlue());

        final DialogComponentBoolean tempTableComp =
            new DialogComponentBoolean(m_config.getSettingsModelTempTable(), "Temporary table");
        box.add(tempTableComp.getComponentPanel());
        box.add(Box.createVerticalGlue());

        final DialogComponentBoolean dropExistingComp =
            new DialogComponentBoolean(m_config.getSettingsModelDropExisting(), "Drop existing table");
        box.add(dropExistingComp.getComponentPanel());
        box.add(Box.createVerticalGlue());

        return panel;
    }

    private JPanel createDynamicPanel() {
        final JPanel panel = new JPanel(new BorderLayout());
        final JTabbedPane tabs = new JTabbedPane();
        tabs.add(m_nameBasedMappingPanel.getTitle(), m_nameBasedMappingPanel);
        tabs.add(m_knimeTypeBasedMappingPanel.getTitle(), m_knimeTypeBasedMappingPanel);
        panel.add(tabs, BorderLayout.CENTER);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_columnsPanel.onSave();
        m_keysPanel.onSave();
        m_nameBasedMappingPanel.onSave();
        m_knimeTypeBasedMappingPanel.onSave();
        m_nameBasedKeysPanel.onSave();
        m_config.saveSettingsForDialog(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        // Prevents opening the dialog if no valid database connection is available
        if (specs[0] == null) {
            throw new NotConfigurableException(
                "Cannot open database table creator without a valid database connection");
        }

        try {
            m_config.loadSettingsForDialog(settings, specs);
        } catch (InvalidSettingsException ex) {

        }

        m_columnsPanel.onLoad();
        m_keysPanel.onLoad();
        m_nameBasedMappingPanel.onLoad();
        m_knimeTypeBasedMappingPanel.onLoad();
        m_nameBasedKeysPanel.onLoad();
    }

}
