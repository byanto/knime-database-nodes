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
public class DBLoopingNodeFactory
        extends NodeFactory<DBLoopingNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public DBLoopingNodeModel createNodeModel() {
        return new DBLoopingNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<DBLoopingNodeModel> createNodeView(final int viewIndex,
            final DBLoopingNodeModel nodeModel) {
        return new DBLoopingNodeView(nodeModel);
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
        return new DBLoopingNodeDialog();
    }

}

