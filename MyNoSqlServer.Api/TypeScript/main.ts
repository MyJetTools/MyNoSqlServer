class main{
    
    private static body : HTMLElement;
    
    private static requested  = false;

    
    static background(){
        
        if (!this.body)
            this.body = document.getElementsByTagName('body')[0];
        
        if (this.requested)
            return;
        
        this.requested = true;
        $.ajax({url:'/api/status', type:'get'})
            .then(result=>{
            this.requested = false;
            this.body.innerHTML = HtmlSubscribersGenerator.generateHtml(result);
        }).fail(()=>{
            this.requested = false;
        })

    }
}

let $:any;

window.setInterval(()=>main.background(), 1000);