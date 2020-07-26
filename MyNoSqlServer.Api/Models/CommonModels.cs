using System;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Api.Models
{
    public static class CommonModels
    {
        private const DataSynchronizationPeriod DefaultSyncPeriod = DataSynchronizationPeriod.Sec5;

        public static DataSynchronizationPeriod ParseSynchronizationPeriodContract(this string data)
        {
            try
            {
                return data?.ParseDataSynchronizationPeriod(DefaultSyncPeriod) ?? DefaultSyncPeriod;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return DefaultSyncPeriod;
            }

        }

    }
}