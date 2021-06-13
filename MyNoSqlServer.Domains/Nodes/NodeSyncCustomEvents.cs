using System;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.TransactionEvents;

namespace MyNoSqlServer.Domains.Nodes
{
    public class InitTableEvent : ITransactionEvent
    {
        public string TableName { get; private set;  }
        public DateTime Happened { get; } = DateTime.UtcNow;
        public TransactionEventAttributes Attributes { get; private set; }
        public DbTable Table { get; private set; }

        public static InitTableEvent Create(TransactionEventAttributes attributes, DbTable table)
        {
            return new InitTableEvent
            {
                Attributes = attributes,
                TableName = table.Name,
                Table = table
            };
        }
    }
}