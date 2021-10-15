var main = /** @class */ (function () {
    function main() {
    }
    main.resize = function () {
        var height = window.innerHeight;
        var width = window.innerWidth;
        if (this.windowHeight == height && this.windowWidth == width)
            return;
        this.windowHeight = height;
        this.windowWidth = width;
        var sbHeight = this.statusBarHeight;
        this.layoutElement.setAttribute('style', 'position:absolute; overflow-y: auto; overflow-x: hidden; ' + this.generatePosition(0, 0, width, height - sbHeight));
        this.statusBarElement.setAttribute('style', 'position:absolute; ' + this.generatePosition(0, height - sbHeight, width, sbHeight));
    };
    main.generatePosition = function (left, top, width, height) {
        return 'top:' + top + 'px; left:' + left + 'px; width:' + width + 'px; height:' + height + 'px';
    };
    main.background = function () {
        var _this = this;
        if (!this.body) {
            this.body = document.getElementsByTagName('body')[0];
            this.body.innerHTML = HtmlSubscribersGenerator.generateLayout();
            this.layoutElement = document.getElementById('main');
            this.statusBarElement = document.getElementById('status-bar');
        }
        this.resize();
        if (this.requested)
            return;
        this.requested = true;
        $.ajax({ url: '/api/status', type: 'get' })
            .then(function (result) {
            _this.requested = false;
            _this.layoutElement.innerHTML = HtmlSubscribersGenerator.generateHtml(result);
            HtmlStatusBar.updateStatusbar(result);
        }).fail(function () {
            _this.requested = false;
            HtmlStatusBar.updateOffline();
        });
    };
    main.requested = false;
    main.statusBarHeight = 24;
    return main;
}());
var $;
window.setInterval(function () { return main.background(); }, 1000);
//# sourceMappingURL=main.js.map