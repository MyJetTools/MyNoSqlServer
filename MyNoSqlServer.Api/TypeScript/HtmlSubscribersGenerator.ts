

class HtmlSubscribersGenerator{
    
    public static generateHtml(data:IStatus):string {

        let masterNode = data.masterNode ? "  Connected to master node: "+data.masterNode : "";
        
        let header = '<table style="width: 100%"><tr>' +
            '<td>Location: '+data.location.id+'</td>' +
            '<td>Compression: '+data.location.compress+'</td><td>'+masterNode+'</td></tr></table>';
        


        return header
            + '<h3>Connected Nodes</h3>'
            + this.generateNodesHtml(data.nodes)
            + '<h3>Readers</h3>'
            + this.generateReadersHtml(data.readers);
    }

    
    private static generateReadersHtml(data: IReaderStatus[]):string{
        let html=`<table class="table table-striped"><tr><th>Id</th><th>Client</th><th>Ip</th><th>tables</th><th></th></tr>`;

        for(let itm of data){

            html += '<tr><td>'+itm.id+'</td><td>'+this.renderName(itm.name)+'</td><td>'+itm.ip+'</td><td>'+this.renderTables(itm.tables)+'</td>' +
                '<td style="font-size: 10px">' +
                '<div><b>C:</b>'+itm.connectedTime+'</div>' +
                '<div><b>L:</b>'+itm.lastIncomingTime+'</div>' +
                '</td></tr>';

        }

        html += '</table>';

        return html;
    }

    private static generateNodesHtml(data: INodeStatus[]):string{
        let html=`<table class="table table-striped"><tr><th>Location</th><th>Connected</th><th>LastAccess</th><th>Compress</th><th>Latency</th></tr>`;

        for(let itm of data){
            html += '<tr><td>'+itm.location+'</td><td>'+itm.connected+'</td><td>'+itm.lastAccessed+'</td><td>'+itm.compress+'</td><td>'+itm.latency+'</td></tr>';
        }

        html += '</table>';

        return html;
    }
    
    

    private static renderName(name:string):string{
        let lines = name.split(';');

        let result = "";
        for (let line of lines){
            result += "<div>"+line+"</div>";
        }

        return result;
    }


    private static renderTables(data:string[]):string{
        let result = "";

        for(let itm of data){
            result += '<span class="badge badge-info" style="margin-left: 5px">'+itm+'</span>';
        }

        return result;

    }
}