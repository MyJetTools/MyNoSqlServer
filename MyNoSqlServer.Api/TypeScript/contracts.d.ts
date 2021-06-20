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

interface ILocationStatus{
    id: string,
    compress: boolean
}


interface IStatus{
    location: ILocationStatus,
    readers:IReaderStatus[],
    nodes: INodeStatus[]
}