using Flurl;
using MyNoSqlServer.Abstractions;

namespace MyNoSqlServer.DataWriter
{
    public static class MyNoSqlDataWriterUtils
    {
        public static string ToHttpContract(this DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            switch (dataSynchronizationPeriod)
            {
                case DataSynchronizationPeriod.Immediately:
                    return "0";
                
                case DataSynchronizationPeriod.Sec1:
                    return "1";
                
                case DataSynchronizationPeriod.Sec5:
                    return "5";

                case DataSynchronizationPeriod.Sec15:
                    return "15";
                
                case DataSynchronizationPeriod.Sec30:
                    return "30";

                case DataSynchronizationPeriod.Min1:
                    return "60";
                
            }

            return "5";
        }

        public static Url AppendDataSyncPeriod(this Url url, DataSynchronizationPeriod dataSynchronizationPeriod)
        {
            return url.SetQueryParam("syncPeriod", dataSynchronizationPeriod.ToHttpContract());
        }

    }
}