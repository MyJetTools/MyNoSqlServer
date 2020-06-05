var main = /** @class */ (function () {
    function main() {
    }
    main.renderTables = function (data) {
        var result = "";
        for (var _i = 0, data_1 = data; _i < data_1.length; _i++) {
            var itm = data_1[_i];
            result += '<span class="badge badge-info" style="margin-left: 5px">' + itm + '</span>';
        }
        return result;
    };
    main.renderHtml = function (data) {
        var html = "<table class=\"table table-striped\"><tr><th>Id</th><th>Client</th><th>Ip</th><th>tables</th><th>Connected</th><th>Last Incoming</th></tr>";
        for (var _i = 0, data_2 = data; _i < data_2.length; _i++) {
            var itm = data_2[_i];
            html += '<tr><td>' + itm.id + '</td><td>' + itm.name + '</td><td>' + itm.ip + '</td><td>' + this.renderTables(itm.tables) + '</td>' +
                '<td>' + itm.connectedTime + '</td><td>' + itm.lastIncomingTime + '</td></tr>';
        }
        html += '</table>';
        return html;
    };
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
            _this.body.innerHTML = _this.renderHtml(result);
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