namespace MyNoSqlServer.Domains
{
    public enum OperationResult
    {
        Ok, RecordNotFound, TableNotFound, TableNameIsEmpty, ShuttingDown, PartitionKeyIsNull, RowKeyIsNull,
        RecordExists, RecordChangedConcurrently, RowNotFound
    }
}