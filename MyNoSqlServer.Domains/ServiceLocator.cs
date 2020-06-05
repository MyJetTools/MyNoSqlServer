using MyNoSqlServer.Domains.DataSynchronization;
using MyNoSqlServer.Domains.SnapshotSaver;

namespace MyNoSqlServer.Domains
{
    public static class ServiceLocator
    {
        
        public static bool ShuttingDown { get; set; }

        public static ISnapshotStorage SnapshotStorage { get; set; }
        
        public static IReplicaSynchronizationService DataSynchronizer;
        
        public static readonly SnapshotSaverEngine SnapshotSaverEngine = new SnapshotSaverEngine();
        
        public static readonly ISnapshotSaverScheduler SnapshotSaverScheduler = new SnapshotSaverScheduler();

    }
}