using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Partitions;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;

namespace  MyNoSqlServer.Domains.TransactionEvents
{

    public interface ITransactionEvent
    {
        string TableName { get;  }
        DateTime Happened { get; }
        TransactionEventAttributes Attributes { get; }
    }
    
    /// <summary>
    /// We delete all existing data from table and initialize it with the new ones
    /// </summary>
    public class InitTableTransactionEvent : ITransactionEvent
    {
        public string TableName { get; private set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }


        public Dictionary<string, List<DbRow>> Snapshot { get; private set; }

        public static InitTableTransactionEvent Create(TransactionEventAttributes attributes,
            DbTable dbTable, Dictionary<string, List<DbRow>> snapshot)
        {
            return new InitTableTransactionEvent
            {
                TableName = dbTable.Name,
                Attributes = attributes,
                Snapshot = snapshot,
            };
        }
        
        public static InitTableTransactionEvent AsDelete(TransactionEventAttributes attributes, DbTable dbTable)
        {
            return new InitTableTransactionEvent
            {
                TableName = dbTable.Name,
                Attributes = attributes,
                Snapshot = new Dictionary<string, List<DbRow>>()
            };
        }
    }

    

    /// <summary>
    /// Replace All the Rows of the Partitions in dictionary to new sets
    /// If there is a partition with no Rows in it - that means Partition is deleted
    /// </summary>
    public class InitPartitionsTransactionEvent : ITransactionEvent
    {
        public string TableName { get; private set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }
        public Dictionary<string, IReadOnlyList<DbRow>> Partitions { get; set; }

        public static InitPartitionsTransactionEvent Create(TransactionEventAttributes attributes, DbTable table, Dictionary<string, IReadOnlyList<DbRow>> partitions)
        {
            return new InitPartitionsTransactionEvent
            {
                TableName = table.Name,
                Partitions = partitions,
                Attributes = attributes
            };
        }
        
        public static InitPartitionsTransactionEvent AsDeletePartition(TransactionEventAttributes attributes, DbTable table, DbPartition sDbPartition)
        {
            return new InitPartitionsTransactionEvent
            {
                TableName = table.Name,
                Attributes = attributes,
                Partitions = new Dictionary<string, IReadOnlyList<DbRow>>
                {
                    [sDbPartition.PartitionKey] = Array.Empty<DbRow>()
                }
        
            };
        }
        
        public static InitPartitionsTransactionEvent AsDeletePartitions(TransactionEventAttributes attributes, DbTable table, IReadOnlyList<DbPartition> dbPartitions)
        {

            var partitions = dbPartitions
                .ToDictionary<DbPartition, string, IReadOnlyList<DbRow>>(
                    dbPartition => dbPartition.PartitionKey, 
                    dbPartition => Array.Empty<DbRow>());

            return new InitPartitionsTransactionEvent
            {
                TableName = table.Name,
                Attributes = attributes,
                Partitions = partitions,
            };
        }
    }
    
    
    /// <summary>
    /// Attributes of table is changed
    /// </summary>

    public class SyncTableAttributes : ITransactionEvent
    {
        public string TableName { get; private set; }
        
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }

        public bool PersistTable { get; set; }
        
        public int MaxPartitionsAmount { get; set; }

        public static SyncTableAttributes Create(TransactionEventAttributes attributes, DbTable dbTable)
        {
            return new SyncTableAttributes
            {
                TableName = dbTable.Name,
                PersistTable = dbTable.Persist,
                Attributes = attributes,
                MaxPartitionsAmount = dbTable.MaxPartitionsAmount,
            };
        }
        
    }

    

    /// <summary>
    /// Some Rows are changed
    /// </summary>
    public class UpdateRowsTransactionEvent : ITransactionEvent
    {
        public string TableName { get; private set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }
        public IReadOnlyList<DbRow> Rows { get; set; }

        public static UpdateRowsTransactionEvent AsRow(TransactionEventAttributes attributes, DbTable table, DbRow row)
        {
            return new UpdateRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = new[] { row }
            };
        }
        
        public static UpdateRowsTransactionEvent AsRows(TransactionEventAttributes attributes, DbTable table, IReadOnlyList<DbRow> rows)
        {
            return new UpdateRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = rows,
            };
        }

    }

    /// <summary>
    /// Some Rows are deleted
    /// </summary>
    public class DeleteRowsTransactionEvent : ITransactionEvent
    {
        public string TableName { get; private set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }
        public IReadOnlyList<DbRow> Rows { get; set; }
        
        public static DeleteRowsTransactionEvent AsRow(TransactionEventAttributes attributes, DbTable table, DbRow row)
        {
            return new DeleteRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = new[] { row }
            };
        }
        
        public static DeleteRowsTransactionEvent AsRows(TransactionEventAttributes attributes, DbTable table, IReadOnlyList<DbRow> rows)
        {
            return new DeleteRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = rows,
            };
        }
    }

}