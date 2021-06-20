interface IReaderStatus{
    id:number;
    ip:string;
    name:string;
    tables:string[];
    connectedTime:string;
    lastIncomingTime:string;
}

interface INodeStatus {
    location: string,
    lastAccessed: string,
    connected: string,
}


interface IStatus{
    readers:IReaderStatus[],
    nodes: INodeStatus[]
}