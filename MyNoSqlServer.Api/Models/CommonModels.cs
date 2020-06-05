using System;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Api.Models
{
    public static class CommonModels
    {
        private const DataSynchronizationPeriod DefaultDataSynchronizationPeriod = DataSynchronizationPeriod.Sec5;

        public static DataSynchronizationPeriod ParseSynchronizationPeriod(this string data)
        {

            if (string.IsNullOrEmpty(data))
                return DefaultDataSynchronizationPeriod;

            if (data == "1")
                return DataSynchronizationPeriod.Sec1;

            if (data == "5")
                return DataSynchronizationPeriod.Sec5;

            if (data == "15")
                return DataSynchronizationPeriod.Sec15;

            if (data == "15")
                return DataSynchronizationPeriod.Sec30;

            if (data == "60")
                return DataSynchronizationPeriod.Min1;

            return DefaultDataSynchronizationPeriod;

        }

    }
}