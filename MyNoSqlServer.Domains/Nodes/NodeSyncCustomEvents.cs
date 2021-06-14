using System;
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
        
        public byte[] TableData { get; set; }
        
        public static FirstInitTableEvent Create(TransactionEventAttributes attributes, DbTable table, byte[] tableData)
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