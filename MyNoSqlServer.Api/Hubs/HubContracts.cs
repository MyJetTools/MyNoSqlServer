using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using MyNoSqlServer.Domains.Db.Rows;

namespace MyNoSqlServer.Api.Hubs
{
    public static class HubContracts
    {

        public static byte[] ToHubUpdateContract(this IReadOnlyList<DbRow> dbRows)
        {
            return dbRows.ToJsonArray().AsArray();
        }

        public static byte[] ToHubDeleteContract(this IReadOnlyList<DbRow> dbRows)
        {
            var result = new Dictionary<string,string>();

            foreach (var dbRow in dbRows)
                result.Add(dbRow.PartitionKey, dbRow.RowKey);

            var json = Newtonsoft.Json.JsonConvert.SerializeObject(result);

            return Encoding.UTF8.GetBytes(json);
        }


        public static int? ContractToLimit(this int limit)
        {
            return limit <= 0 ? (int?) null : limit;
        }
        public static int? ContractToSkip(this int take)
        {
            return take <= 0 ? (int?) null : take;
        }   
    }
}