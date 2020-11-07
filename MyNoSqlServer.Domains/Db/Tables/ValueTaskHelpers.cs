using System.Threading.Tasks;

namespace MyNoSqlServer.Domains.Db.Tables
{
    public static class ValueTaskHelpers
    {
        public static async ValueTask<T> ReturnValueTaskResult<T>(this ValueTask taskToAwait, T resultToCast)
        {
            await taskToAwait;
            return resultToCast;
        }
        
    }
}