using System.Collections.Generic;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Domains.TransactionEvents
{
    public static class TransactionEventsToArrayMapper
    {

        public static byte[] AsByteArray(this Dictionary<string, List<DbRow>> snapshot)
        {
            var result = new List<DbRow>();

                foreach (var partitionRows in snapshot.Values)
                    result.AddRange(partitionRows);

                return result.ToJsonArray().AsArray();
        }
        
        public static byte[] AsByteArray(this Dictionary<string, IReadOnlyList<DbRow>> snapshot)
        {
            var result = new List<DbRow>();

            foreach (var partitionRows in snapshot.Values)
                result.AddRange(partitionRows);

            return result.ToJsonArray().AsArray();
        }
        
    }
}