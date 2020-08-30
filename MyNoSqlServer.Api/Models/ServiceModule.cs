using Autofac;

namespace MyNoSqlServer.Api.Models
{
    public class ServiceModule: Module
    {
        private readonly string _backupAzureConnectString;

        public ServiceModule(string backupAzureConnectString)
        {
            _backupAzureConnectString = backupAzureConnectString;
        }

        protected override void Load(ContainerBuilder builder)
        {
            /* FOR EXAMPLE
             
            register instance 
             
            builder
                .RegisterInstance(new AzureBlobSnapshotStorage(_backupAzureConnectString))
                .As<ISnapshotStorage>()
                .SingleInstance();

            register class
            link to class you can claim in constructor
            if class has dependency then all dependencies resolve from container

            builder
                .RegisterType<AzureBlobSnapshotStorage>()
                .As<ISnapshotStorage>()
                .SingleInstance();

            */
        }
    }
}