using System;

namespace MyNoSqlServer.Domains
{
    public interface IMetricsWriter
    {
        void WriteExpiredEntitiesGcDuration(TimeSpan timeSpan);
    }
}