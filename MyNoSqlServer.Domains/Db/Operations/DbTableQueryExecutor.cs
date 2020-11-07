using System.Collections.Generic;
using System.Linq;
using MyNoSqlServer.Domains.Db.Rows;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Query;

namespace MyNoSqlServer.Domains.Db.Operations
{
    public static class DbTableQueryExecutor
    {
        public static IReadOnlyList<DbRow> ExecQuery(this DbTable dbTable, IEnumerable<QueryCondition> queryConditions)
        {
            var conditionsDict = queryConditions
                .GroupBy(itm => itm.FieldName)
                .ToDictionary(itm => itm.Key, itm => itm.ToList());

            var result = new List<DbRow>();
            
            dbTable.GetAccessWithReadLock(dbTableReader =>
            {
                var partitions = conditionsDict.ContainsKey(RowJsonUtils.PartitionKeyFieldName)
                    ? dbTableReader.Partitions.FilterByQueryConditions(conditionsDict[RowJsonUtils.PartitionKeyFieldName]).ToList()
                    : dbTableReader.Partitions.Values.ToList();

                if (conditionsDict.ContainsKey(RowJsonUtils.PartitionKeyFieldName))
                    conditionsDict.Remove(RowJsonUtils.PartitionKeyFieldName);
                
                foreach (var partition in partitions) 
                    result.AddRange(partition.ApplyQuery(conditionsDict));
            });

            return result;
        }
    }
}