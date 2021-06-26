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


interface IQueuesStatus{
    persistence: number
}

interface IStatus{
    masterNode: string,
    queues:IQueuesStatus,
    location: ILocationStatus,
    readers:IReaderStatus[],
    nodes: INodeStatus[]
}