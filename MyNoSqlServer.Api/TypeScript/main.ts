class main{
    
    private static body : HTMLElement;
    
    private static requested  = false;
    
    private static renderTables(data:string[]):string{
        let result = "";
        
        for(let itm of data){
            result += '<span class="badge badge-info" style="margin-left: 5px">'+itm+'</span>';
        }
        
        
        return result;
        
    }
    
    private static renderName(name:string):string{
        let lines = name.split(';');
        
        let result = "";
        for (let line of lines){
            result += "<div>"+line+"</div>";
        }
        
        return result;
    }
    
    private static renderHtml(data:IStatus[]):string{

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
    
    static background(){
        
        if (!this.body)
            this.body = document.getElementsByTagName('body')[0];
        
        if (this.requested)
            return;
        
        this.requested = true;
        $.ajax({url:'/api/status', type:'get'})
            .then(result=>{
            this.requested = false;
            this.body.innerHTML = this.renderHtml(result);
        }).fail(()=>{
            this.requested = false;
        })
        

    }
}

let $:any;

window.setInterval(()=>main.background(), 1000);