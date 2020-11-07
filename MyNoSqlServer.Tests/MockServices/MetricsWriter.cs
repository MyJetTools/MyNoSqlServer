using System;
using MyNoSqlServer.Domains;

namespace MyNoSqlServer.Tests.MockServices
{
    public class MetricsWriter : IMetricsWriter
    {
        public void WriteExpiredEntitiesGcDuration(TimeSpan timeSpan)
        {
            Console.WriteLine("WrittenMetric WriteExpiredEntitiesGcDuration: "+timeSpan);
        }
    }
}