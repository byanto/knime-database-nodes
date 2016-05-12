package org.knime.base.node.io.database.looper;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "DBLooper" Node.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class DBLooperNodeFactory
        extends NodeFactory<DBLooperNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public DBLooperNodeModel createNodeModel() {
        return new DBLooperNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<DBLooperNodeModel> createNodeView(final int viewIndex,
            final DBLooperNodeModel nodeModel) {
        return new DBLooperNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new DBLooperNodeDialog();
    }

}

