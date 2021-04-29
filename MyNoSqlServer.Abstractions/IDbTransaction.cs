

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
    
    public interface IInsertOrReplaceEntitiesTransactionAction: IDbTransactionAction
    {
        List<byte[]> Entities { get;  }
    }


}