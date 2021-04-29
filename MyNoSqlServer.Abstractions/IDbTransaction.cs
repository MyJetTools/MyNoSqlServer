

using System.Collections.Generic;

namespace MyNoSqlServer.Abstractions
{
    public interface IDbTransactionAction
    {
        string TableName { get; set; }
    }

    public interface ICleanTableTransactionAction : IDbTransactionAction
    {
        
    }

    public interface IDeletePartitionsTransactionAction: IDbTransactionAction
    {
        string[] PartitionKeys { get;  }
    }

    public interface IDeleteRowsTransactionAction: IDbTransactionAction
    {
        string PartitionKey { get; }
        string[] RowKeys { get;  }
    }


    public enum DbEntityType
    {
        Json, Protobuf
    }
    
    public interface IInsertOrReplaceEntitiesTransactionAction: IDbTransactionAction
    {
        IEnumerable<(DbEntityType Type, byte[] Payload)> Entities { get;  }
    }


}