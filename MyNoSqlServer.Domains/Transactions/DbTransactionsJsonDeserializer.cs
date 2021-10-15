using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Transactions
{
    public static class DbTransactionsJsonDeserializer
    {
        public static IEnumerable<IDbTransactionAction> GetTransactions(IMyMemory memory)
        {
            foreach (var transactionItem in memory.SplitJsonArrayToObjects())
            {
                var json = Encoding.UTF8.GetString(transactionItem.Span);
                
                var baseTransaction = JsonSerializer.Deserialize<BaseTransaction>(json);

                if (baseTransaction == null)
                    throw new Exception("Can not deserialize transaction");

                if (baseTransaction.Type == CleanTableTransactionJsonModel.Id)
                    yield return JsonSerializer.Deserialize<CleanTableTransactionJsonModel>(json);

                if (baseTransaction.Type == DeletePartitionsTransactionActionJsonModel.Id)
                    yield return JsonSerializer.Deserialize<DeletePartitionsTransactionActionJsonModel>(json);

                if (baseTransaction.Type == DeleteRowsTransactionJsonModel.Id)
                    yield return JsonSerializer.Deserialize<DeleteRowsTransactionJsonModel>(json);

                if (baseTransaction.Type == InsertOrReplaceEntitiesTransactionJsonModel.Id)
                {
                    var result = new InsertOrReplaceEntitiesTransactionJsonModel
                    {
                        Type = baseTransaction.Type,
                        Entities = new List<byte[]>()
                    };

                    var firstLines = transactionItem.ParseFirstLine();

                    var mem = new MyMemoryAsByteArray(firstLines["Entities"].Value.ToArray());
                    
                    foreach (var entity in mem.SplitJsonArrayToObjects())
                    {
                        result.Entities.Add(entity.AsArray());
                    }

                    yield return result;
                }
            }

        } 
        
    }
}