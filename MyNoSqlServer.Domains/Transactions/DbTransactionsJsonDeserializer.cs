using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Json;

namespace MyNoSqlServer.Domains.Transactions
{
    public static class DbTransactionsJsonDeserializer
    {
        public static IEnumerable<IDbTransaction> GetTransactions(IMyMemory memory)
        {
            foreach (var transactionItem in memory.SplitJsonArrayToObjects())
            {
                var json = Encoding.UTF8.GetString(transactionItem.Span);
                
                var baseTransaction = JsonSerializer.Deserialize<BaseTransaction>(json);

                if (baseTransaction == null)
                    throw new Exception("Can not deserialize transaction");

                if (baseTransaction.Type == CleanTableTransaction.Id)
                    yield return JsonSerializer.Deserialize<CleanTableTransaction>(json);

                if (baseTransaction.Type == CleanPartitionsTransaction.Id)
                    yield return JsonSerializer.Deserialize<CleanPartitionsTransaction>(json);

                if (baseTransaction.Type == DeleteRowsTransaction.Id)
                    yield return JsonSerializer.Deserialize<DeleteRowsTransaction>(json);

                if (baseTransaction.Type == InsertOrReplaceEntitiesTransaction.Id)
                {
                    var result = new InsertOrReplaceEntitiesTransaction
                    {
                        Type = baseTransaction.Type,
                        Entities = new List<DynamicEntity>()
                    };

                    var firstLines = transactionItem.ParseFirstLine();

                    var mem = new MyMemoryAsByteArray(firstLines["Entities"].Value.ToArray());
                    
                    foreach (var entity in mem.SplitJsonArrayToObjects())
                    {
                        result.Entities.Add(entity.ParseDynamicEntity());
                    }

                    yield return result;
                }
            }

        } 
        
    }
}