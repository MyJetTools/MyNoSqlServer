using System;
using Microsoft.Extensions.Primitives;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.Api.Models
{
    public static class CommonModels
    {
        public const DataSynchronizationPeriod DefaultSyncPeriod = DataSynchronizationPeriod.Sec5;

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


        public static DataSynchronizationPeriod ParseSynchronizationPeriodContract(this StringValues values)
        {
                return ParseSynchronizationPeriodContract(DefaultSyncPeriod.ToString());
        }

    }
}