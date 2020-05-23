/* eslint-disable no-await-in-loop */
const ConnectionClient = require('./connection-client');

/**
 * Execute a query using batch/statement infrastructure
 * Batch must already be created.
 * Returns last statement result on finish to remain compatible with old "query-result" use
 * @param {Object} config
 * @param {import('../models/index')} models
 * @param {string} batchId
 */
async function executeBatch(config, models, batchId) {
  const batch = await models.batches.findOneById(batchId);
  const user = await models.users.findOneById(batch.userId);
  const connection = await models.connections.findOneById(batch.connectionId);

  // Get existing connectionClient if one was specified to use per batch
  // Otherwise create a new one and connect it if the ConnectionClient supports it.
  // If a new connectionClient was created *and* connected, take note to disconnect later
  let connectionClient;
  let disconnectOnFinish = false;
  if (batch.connectionClientId) {
    connectionClient = models.connectionClients.getOneById(
      batch.connectionClientId
    );
    if (!connectionClient) {
      throw new Error('Connection client disconnected');
    }
  } else {
    connectionClient = new ConnectionClient(connection, user);
    // If connectionClient supports the "Client" driver,
    // and it is not connected, connect it
    if (connectionClient.Client && !connectionClient.isConnected()) {
      await connectionClient.connect();
      disconnectOnFinish = true;
    }
  }

  // run statements
  let queryResult;
  let statementError;
  for (const statement of batch.statements) {
    try {
      await models.statements.updateStarted(statement.id);
      queryResult = await connectionClient.runQuery(statement.statementText);
      await models.statements.updateFinished(statement.id, queryResult);
    } catch (error) {
      statementError = error;
      await models.statements.updateErrored(statement.id, {
        title: error.message,
      });
      await models.batches.updateStatus(batch.id, 'error');
      break;
    }
  }

  if (!statementError) {
    await models.batches.updateStatus(batch.id, 'finished');
  }

  if (disconnectOnFinish) {
    await connectionClient.disconnect();
  }

  // Log query history for legacy purposes
  // This may be able to be replaced by admin view using batches/statements
  if (config.get('queryHistoryRetentionTimeInDays') > 0) {
    await models.queryHistory.save({
      userId: user ? user.id : 'unauthenticated link',
      userEmail: user ? user.email : 'anauthenticated link',
      connectionId: connection.id,
      connectionName: connection.name,
      startTime: queryResult.startTime,
      stopTime: queryResult.stopTime,
      queryRunTime: queryResult.queryRunTime,
      queryId: batch.queryId,
      queryName: batch.name,
      queryText: batch.selectedText,
      incomplete: queryResult.incomplete,
      rowCount: queryResult.rows.length,
    });
  }

  // For /api/query-result compatibility, either throw error if it exists, or return last queryResult
  if (statementError) {
    throw statementError;
  }
  return queryResult;
}

module.exports = executeBatch;