using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.DataReadersTcpServer
{
    public static class ContractsMappers
    {

        public static byte[] ToReaderBytes(this Dictionary<string, List<DbRow>> dbRows)
        {
            var result = new List<DbRow>();

            foreach (var partitionRows in dbRows.Values)
                result.AddRange(partitionRows);

            return result.ToJsonArray().AsArray();
        }
        
    }
}