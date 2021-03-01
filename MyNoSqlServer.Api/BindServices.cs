using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Domains.DataSynchronization;

namespace MyNoSqlServer.Api
{
    public static class BindServices
    {

        public static void BindApiServices(this IServiceCollection services)
        {
            services.AddSingleton<IReplicaSynchronizationService>(new ChangesPublisherToSocket());
        }
        
    }
}