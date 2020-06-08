namespace MyNoSqlServer.Abstractions
{
    public enum OperationResult
    {
        Ok, RecordNotFound, TableNotFound, TableNameIsEmpty, 
        ShuttingDown, PartitionKeyIsNull, 
        RowKeyIsNull, RecordExists, RecordChangedConcurrently, 
        RowNotFound, CanNotCreateObject, Canceled, QueryIsNull
    }
}