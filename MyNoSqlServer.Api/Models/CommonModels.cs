using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Api.Models
{
    public static class CommonModels
    {
        public static DataSynchronizationPeriod ParseSynchronizationPeriodContract(this string data)
        {
            return data.ParseDataSynchronizationPeriod(DataSynchronizationPeriod.Sec5);
        }

    }
}