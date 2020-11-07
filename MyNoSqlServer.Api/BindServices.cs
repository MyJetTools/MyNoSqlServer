using MyDependencies;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Domains;
using MyNoSqlServer.Domains.DataSynchronization;

namespace MyNoSqlServer.Api
{
    public static class BindServices
    {
        public static void BindApiServices(this IServiceRegistrator sr)
        {
            sr.Register<IReplicaSynchronizationService>(new ChangesPublisherToSocket());
            sr.Register<IMetricsWriter>(new PrometheusMetricsWriter());
        }
        
    }
}