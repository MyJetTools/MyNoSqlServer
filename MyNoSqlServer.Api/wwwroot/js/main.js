var main = /** @class */ (function () {
    function main() {
    }
    main.background = function () {
        var _this = this;
        if (!this.body)
            this.body = document.getElementsByTagName('body')[0];
        if (this.requested)
            return;
        this.requested = true;
        $.ajax({ url: '/api/status', type: 'get' })
            .then(function (result) {
            _this.requested = false;
            _this.body.innerHTML = HtmlSubscribersGenerator.generateHtml(result);
        }).fail(function () {
            _this.requested = false;
        });
    };
    main.requested = false;
    return main;
}());
var $;
window.setInterval(function () { return main.background(); }, 1000);
//# sourceMappingURL=main.js.map