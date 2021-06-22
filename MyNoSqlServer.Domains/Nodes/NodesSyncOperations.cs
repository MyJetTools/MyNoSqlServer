using MyNoSqlServer.Domains.Db;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Nodes
{
    public class NodesSyncOperations
    {
        private readonly SyncEventsDispatcher _syncEventsDispatcher;
        private readonly DbInstance _dbInstance;

        public NodesSyncOperations(SyncEventsDispatcher syncEventsDispatcher, DbInstance dbInstance)
        {
            _syncEventsDispatcher = syncEventsDispatcher;
            _dbInstance = dbInstance;
        }



        public void SetTableAttributes(UpdateTableAttributesTransactionEvent tableAttributesEvent)
        {

            var dbTable = _dbInstance.TryGetTable(tableAttributesEvent.TableName);

            var tableCreated = false;

            if (dbTable == null)
                dbTable = _dbInstance.GetWriteAccess(writeAccess =>
                {
                    var result = writeAccess.TryGetTable(tableAttributesEvent.TableName);
                    if (result != null)
                        return result;

                    tableCreated = true;

                    return writeAccess.CreateTable(tableAttributesEvent.TableName, tableAttributesEvent.PersistTable,
                        tableAttributesEvent.MaxPartitionsAmount);

                });


            var set = dbTable.SetAttributes(tableAttributesEvent.PersistTable,
                tableAttributesEvent.MaxPartitionsAmount);

            if ((tableCreated || set) && tableAttributesEvent.Attributes != null)
                _syncEventsDispatcher.Dispatch(tableAttributesEvent);
        }


        public void ReplaceTable(InitTableTransactionEvent initTableTransactionEvent)
        {

            var dbTable = _dbInstance.GetTable(initTableTransactionEvent.TableName);

            dbTable.GetWriteAccess(writeAccess =>
            {
                writeAccess.InitTable(initTableTransactionEvent.Snapshot);
                
                if (initTableTransactionEvent.Attributes != null)
                    _syncEventsDispatcher.Dispatch(initTableTransactionEvent);
            });
        }

        public void ReplacePartitions(InitPartitionsTransactionEvent initPartitionsTransactionEvent)
        {
            var dbTable = _dbInstance.GetTable(initPartitionsTransactionEvent.TableName);

            dbTable.GetWriteAccess(writeAccess =>
            {
                foreach (var (partitionKey, rows) in initPartitionsTransactionEvent.Partitions)
                {
                    var partition = writeAccess.GetOrCreatePartition(partitionKey);
                    partition.ClearAndBulkInsertOrReplace(rows);
                }

                if (initPartitionsTransactionEvent.Attributes != null)
                    _syncEventsDispatcher.Dispatch(initPartitionsTransactionEvent);
            });
        }

        public void UpdateRows(UpdateRowsTransactionEvent updateRowsTransactionEvent)
        {
            var dbTable = _dbInstance.GetTable(updateRowsTransactionEvent.TableName);

            dbTable.GetWriteAccess(writeAccess =>
            {
                foreach (var (partitionKey, rows) in updateRowsTransactionEvent.RowsByPartition)
                {
                    var partition = writeAccess.GetOrCreatePartition(partitionKey);
                    partition.BulkInsertOrReplace(rows);
                }

                _syncEventsDispatcher.Dispatch(updateRowsTransactionEvent);
            });
        }

        public void DeleteRows(DeleteRowsTransactionEvent deleteRowsTransactionEvent)
        {
            var dbTable = _dbInstance.GetTable(deleteRowsTransactionEvent.TableName);

            dbTable.GetWriteAccess(writeAccess =>
            {
                foreach (var (partitionKey, rows) in deleteRowsTransactionEvent.Rows)
                {
                    var partition = writeAccess.GetOrCreatePartition(partitionKey);

                    foreach (var rowKey in rows)
                        partition.DeleteRow(rowKey);
                }

                _syncEventsDispatcher.Dispatch(deleteRowsTransactionEvent);
            });
        }


    }
}