using System;
using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Common;
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
        public string TableName { get; set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; set; }


        public IReadOnlyDictionary<string, IReadOnlyList<DbRow>> Snapshot { get; set; }

        public static InitTableTransactionEvent Create(TransactionEventAttributes attributes,
            DbTable dbTable, IReadOnlyDictionary<string, IReadOnlyList<DbRow>> snapshot)
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
                Snapshot = new Dictionary<string, IReadOnlyList<DbRow>>()
            };
        }
    }

    

    /// <summary>
    /// Replace All the Rows of the Partitions in dictionary to new sets
    /// If there is a partition with no Rows in it - that means Partition is deleted
    /// </summary>
    public class InitPartitionsTransactionEvent : ITransactionEvent
    {
        public string TableName { get; set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; set; }
        public IReadOnlyDictionary<string, IReadOnlyList<DbRow>> Partitions { get; set; }

        public static InitPartitionsTransactionEvent Create(TransactionEventAttributes attributes, DbTable table, IReadOnlyDictionary<string, IReadOnlyList<DbRow>> partitions)
        {
            return new InitPartitionsTransactionEvent
            {
                TableName = table.Name,
                Partitions = partitions,
                Attributes = attributes
            };
        }
        
        public static InitPartitionsTransactionEvent AsInitPartition(TransactionEventAttributes attributes, DbTable table, IPartitionWriteAccess partition)
        {
            return new InitPartitionsTransactionEvent
            {
                TableName = table.Name,
                Partitions = new Dictionary<string, IReadOnlyList<DbRow>>
                {
                    [partition.PartitionKey] = partition.GetAllRows()
                },
                Attributes = attributes
            };
        }
        
        public static InitPartitionsTransactionEvent AsDeletePartition(TransactionEventAttributes attributes, DbTable table, DbPartition dbPartition)
        {
            return new InitPartitionsTransactionEvent
            {
                TableName = table.Name,
                Attributes = attributes,
                Partitions = new Dictionary<string, IReadOnlyList<DbRow>>
                {
                    [dbPartition.PartitionKey] = Array.Empty<DbRow>()
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

    public class UpdateTableAttributesTransactionEvent : ITransactionEvent
    {
        public string TableName { get; set; }
        
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; set; }

        public bool PersistTable { get; set; }
        
        public int MaxPartitionsAmount { get; set; }

        public static UpdateTableAttributesTransactionEvent Create(TransactionEventAttributes attributes, DbTable dbTable)
        {
            return new UpdateTableAttributesTransactionEvent
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
        public string TableName { get; set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; set; }
        public IReadOnlyDictionary<string, IReadOnlyList<DbRow>> RowsByPartition { get; set; }

        public static UpdateRowsTransactionEvent AsRow(TransactionEventAttributes attributes, DbTable table, DbRow row)
        {
            return new UpdateRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                RowsByPartition = new Dictionary<string, IReadOnlyList<DbRow>>
                {
                    [row.PartitionKey] = new []{row}
                }
            };
        }
        
        public static UpdateRowsTransactionEvent AsRows(TransactionEventAttributes attributes, DbTable table, IReadOnlyDictionary<string, IReadOnlyList<DbRow>> rows)
        {
            return new UpdateRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                RowsByPartition = rows,
            };
        }

    }

    /// <summary>
    /// Some Rows are deleted
    /// </summary>
    public class DeleteRowsTransactionEvent : ITransactionEvent
    {
        public string TableName { get; set; }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; set; }
        public IReadOnlyDictionary<string, IReadOnlyList<string>> Rows { get; set; }
        
        public static DeleteRowsTransactionEvent AsRow(TransactionEventAttributes attributes, DbTable table, DbRow dbRow)
        {
            return new DeleteRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = new Dictionary<string, IReadOnlyList<string>>
                {
                    [dbRow.PartitionKey] = new []{dbRow.RowKey}
                }
            };
        }
        
        public static DeleteRowsTransactionEvent AsRows(TransactionEventAttributes attributes, DbTable table, IReadOnlyList<DbRow> dbDbRows)
        {
            return new DeleteRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = dbDbRows.GroupBy(itm => itm.PartitionKey).ToDictionary(
                    itm => itm.Key, 
                    itm=> itm.Select(dbRow => dbRow.RowKey).AsReadOnlyList()),
            };
        }
        
        public static DeleteRowsTransactionEvent AsRows(TransactionEventAttributes attributes, DbTable table, IReadOnlyDictionary<string, List<DbRow>> rows)
        {
            return new DeleteRowsTransactionEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Rows = rows.ToDictionary(
                    itm => itm.Key, 
                    itm=> itm.Value.Select(dbRow => dbRow.RowKey).AsReadOnlyList()),
            };
        }
    }

}