using System;
using MyNoSqlServer.Domains;

namespace MyNoSqlServer.Api.Services
{
    public class PrometheusMetricsWriter : IMetricsWriter
    {
        public void WriteExpiredEntitiesGcDuration(TimeSpan timeSpan)
        {
            
        }
    }
}