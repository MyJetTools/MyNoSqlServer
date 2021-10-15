using System;
using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Nodes
{
    public class FirstInitTableEvent : ITransactionEvent
    {
        public string TableName { get; private set;  }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }
        public DbTable Table { get; private set; }
        
        public IReadOnlyDictionary<string, IReadOnlyList<DbRow>> TableData { get; set; }
        
        public static FirstInitTableEvent Create(TransactionEventAttributes attributes, DbTable table, IReadOnlyDictionary<string, IReadOnlyList<DbRow>> tableData)
        {
            return new FirstInitTableEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Table = table,
                TableData = tableData
            };
        }
    }
}