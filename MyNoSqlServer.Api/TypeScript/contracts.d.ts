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
    compress: boolean
    latency: string
}

interface ILocationStatus{
    id: string,
    compress: boolean
}


interface IStatus{
    masterNode: string,
    location: ILocationStatus,
    readers:IReaderStatus[],
    nodes: INodeStatus[]
}